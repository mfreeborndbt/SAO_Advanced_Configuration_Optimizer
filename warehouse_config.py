"""Warehouse size mapping and cost calculation.

Cost model:
  - Base rate: configurable $/second on smallest tier (default $0.00056)
  - Each size tier doubles the cost
"""

import json
import os
from discovery_client import DbtClient

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")

WAREHOUSE_SIZES = [
    {"id": "XS",  "snowflake": "XS",  "databricks": "2XS", "multiplier": 1},
    {"id": "S",   "snowflake": "S",   "databricks": "XS",  "multiplier": 2},
    {"id": "M",   "snowflake": "M",   "databricks": "S",   "multiplier": 4},
    {"id": "L",   "snowflake": "L",   "databricks": "M",   "multiplier": 8},
    {"id": "XL",  "snowflake": "XL",  "databricks": "L",   "multiplier": 16},
    {"id": "2XL", "snowflake": "2XL", "databricks": "XL",  "multiplier": 32},
    {"id": "3XL", "snowflake": "3XL", "databricks": "2XL", "multiplier": 64},
    {"id": "4XL", "snowflake": "4XL", "databricks": "3XL", "multiplier": 128},
]

DEFAULT_BASE_COST = 0.00056  # Smallest tier rate per second

COST_MULTIPLIERS = {s["id"]: s["multiplier"] for s in WAREHOUSE_SIZES}


def _config_path(env_key):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    return os.path.join(CONFIG_DIR, f"{env_key}_warehouses.json")


def get_warehouse_config(env_key):
    """Load saved warehouse config (size mapping + base cost) for an environment."""
    path = _config_path(env_key)
    if os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        # Handle legacy format (flat dict of warehouse->size)
        if "mapping" not in data:
            return {"mapping": data, "base_cost": DEFAULT_BASE_COST}
        return data
    return {"mapping": {}, "base_cost": DEFAULT_BASE_COST}


def save_warehouse_config(env_key, mapping, base_cost):
    """Save warehouse config."""
    path = _config_path(env_key)
    with open(path, "w") as f:
        json.dump({"mapping": mapping, "base_cost": base_cost}, f, indent=2)


def get_warehouse_mapping(env_key):
    """Convenience: return just the mapping dict."""
    return get_warehouse_config(env_key)["mapping"]


def get_base_cost(env_key):
    """Convenience: return the base cost per second."""
    return get_warehouse_config(env_key)["base_cost"]


def discover_warehouses(client: DbtClient):
    """Find all distinct warehouses used across the environment.

    Returns dict of warehouse_name -> model_count.
    Sources:
    1. Connection default
    2. Per-model snowflake_warehouse from manifest.json
    """
    warehouses = {}

    # Get connection default
    try:
        env_data = client.admin_get_v3(f"environments/{client.environment_id}/")
        conn_details = env_data["data"].get("project", {}).get("connection", {}).get("details", {})
        default_wh = conn_details.get("warehouse")
        if default_wh:
            warehouses[default_wh.upper()] = {"count": 0, "is_default": True}
    except Exception:
        default_wh = None

    # Get latest successful run's manifest
    from history import fetch_scheduled_runs
    runs = fetch_scheduled_runs(client, days=7)
    latest = next((r for r in runs if r["status"] == 10), None)

    if latest:
        try:
            manifest = client.admin_get(f"runs/{latest['id']}/artifacts/manifest.json")
            for uid, node in manifest.get("nodes", {}).items():
                if node.get("resource_type") != "model":
                    continue
                config = node.get("config") or {}
                mat = config.get("materialized")
                if mat not in ("table", "incremental"):
                    continue
                wh = config.get("snowflake_warehouse")
                if wh:
                    wh_upper = wh.upper()
                    if wh_upper not in warehouses:
                        warehouses[wh_upper] = {"count": 0, "is_default": False}
                    warehouses[wh_upper]["count"] += 1
                elif default_wh:
                    warehouses[default_wh.upper()]["count"] += 1
        except Exception as e:
            print(f"Warning: Could not read manifest: {e}")

    return warehouses


def cost_per_second(warehouse_name, mapping, base_cost):
    """Get cost per second for a warehouse based on its mapped size."""
    size = mapping.get(warehouse_name.upper(), "XS")
    multiplier = COST_MULTIPLIERS.get(size, 1)
    return base_cost * multiplier


def calculate_model_costs(models, mapping, base_cost):
    """Add cost fields to model dicts based on warehouse mapping."""
    for m in models:
        wh = (m.get("warehouse") or "").upper()
        cps = cost_per_second(wh, mapping, base_cost)

        avg_time = m.get("avg_execution_time")
        min_time = m.get("min_execution_time")
        max_time = m.get("max_execution_time")
        total_time = m.get("total_execution_time")

        m["cost_per_second"] = cps
        m["avg_cost"] = avg_time * cps if avg_time is not None else None
        m["min_cost"] = min_time * cps if min_time is not None else None
        m["max_cost"] = max_time * cps if max_time is not None else None
        m["total_cost"] = total_time * cps if total_time is not None else None

    return models
