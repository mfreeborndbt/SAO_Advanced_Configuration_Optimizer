import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash
from discovery_client import load_credentials, save_credentials, get_client_from_config
from history import build_aggregate
from warehouse_config import (
    discover_warehouses,
    get_warehouse_config,
    save_warehouse_config,
    calculate_model_costs,
    WAREHOUSE_SIZES,
    DEFAULT_BASE_COST,
)
from recommendation_engine import generate_recommendations

app = Flask(__name__)
app.secret_key = "dbt-observability-setup-key"


def _env_key_from_config():
    """Derive a sanitized env_key from the configured credentials."""
    creds = load_credentials()
    if creds is None:
        return "default"
    host = creds.get("host_url", "default")
    return re.sub(r"[^a-zA-Z0-9_-]", "_", host.split(".")[0])


def fmt_duration(seconds):
    """Format seconds into a human-readable duration."""
    if seconds is None:
        return "\u2014"
    s = float(seconds)
    if s < 60:
        return f"{s:.0f}s" if s >= 1 else f"{s:.2f}s"
    elif s < 3600:
        m = int(s // 60)
        sec = int(s % 60)
        return f"{m}m {sec}s" if sec else f"{m}m"
    elif s < 86400:
        h = int(s // 3600)
        m = int((s % 3600) // 60)
        return f"{h}h {m}m" if m else f"{h}h"
    else:
        d = int(s // 86400)
        h = int((s % 86400) // 3600)
        return f"{d}d {h}h" if h else f"{d}d"


def fmt_cost(dollars):
    """Format a dollar amount."""
    if dollars is None:
        return "\u2014"
    d = float(dollars)
    if d < 0.01:
        return f"${d:.4f}"
    elif d < 1:
        return f"${d:.2f}"
    elif d < 1000:
        return f"${d:,.2f}"
    else:
        return f"${d:,.0f}"


app.jinja_env.filters["duration"] = fmt_duration
app.jinja_env.filters["cost"] = fmt_cost


def _load_models_with_costs():
    """Shared data loader for dashboard and recommendations.

    Returns (project_name, models, total_runs, has_costs, env_key, account_name)
    or None if not configured.
    """
    creds = load_credentials()
    if creds is None:
        return None

    env_key = _env_key_from_config()
    client = get_client_from_config()

    project_name, models, total_runs, high_cost_ids, sao_status = build_aggregate(
        client, days=7, max_models=500,
    )

    wh_config = get_warehouse_config(env_key)
    mapping = wh_config["mapping"]
    base_cost = wh_config["base_cost"]
    models = calculate_model_costs(models, mapping, base_cost)
    has_costs = bool(mapping)

    if has_costs:
        cost_lookup = {m["unique_id"]: m.get("avg_cost") for m in models}
        for m in models:
            downstream_uids = m.get("_downstream_uids", [])
            if downstream_uids:
                costs = [cost_lookup[uid] for uid in downstream_uids if uid in cost_lookup and cost_lookup[uid] is not None]
                m["downstream_avg_cost"] = sum(costs) if costs else None
            upstream_uids = m.get("_upstream_uids", [])
            if upstream_uids:
                costs = [cost_lookup[uid] for uid in upstream_uids if uid in cost_lookup and cost_lookup[uid] is not None]
                m["upstream_avg_cost"] = sum(costs) if costs else None

    account_name = creds.get("name", "")
    return project_name, models, total_runs, has_costs, env_key, account_name, sao_status


@app.route("/")
def index():
    result = _load_models_with_costs()
    if result is None:
        return redirect(url_for("setup"))

    project_name, models, total_runs, has_costs, env_key, account_name, sao_status = result

    return render_template(
        "index.html",
        project_name=project_name,
        env_name=account_name,
        env_key=env_key,
        models=models,
        total_runs=total_runs,
        has_costs=has_costs,
        active_tab="dashboard",
    )


@app.route("/recommendations")
def recommendations():
    result = _load_models_with_costs()
    if result is None:
        return redirect(url_for("setup"))

    project_name, models, total_runs, has_costs, env_key, account_name, sao_status = result

    recs = generate_recommendations(models, has_costs, sao_status=sao_status)

    # Filter sao_status for display: only show model-building jobs, not maintenance/CI
    from recommendation_engine import _is_model_building_job
    display_sao_status = {
        **sao_status,
        "disabled_jobs": [j for j in sao_status["disabled_jobs"] if _is_model_building_job(j)],
    }
    display_sao_status["all_enabled"] = len(display_sao_status["disabled_jobs"]) == 0

    total_savings = sum(r["estimated_impact"]["weekly_savings"] or 0 for r in recs)
    total_runs_saveable = sum(r["estimated_impact"]["runs_eliminated"] or 0 for r in recs)
    high_priority_count = sum(1 for r in recs if r["priority"] == "high")

    return render_template(
        "recommendations.html",
        project_name=project_name,
        env_name=account_name,
        models=models,
        total_runs=total_runs,
        has_costs=has_costs,
        recommendations=recs,
        total_savings=total_savings,
        total_runs_saveable=total_runs_saveable,
        high_priority_count=high_priority_count,
        sao_status=display_sao_status,
        active_tab="recommendations",
    )


@app.route("/setup")
def setup():
    creds = load_credentials()
    return render_template("setup.html", creds=creds)


@app.route("/setup/save", methods=["POST"])
def setup_save():
    data = {
        "host_url": request.form.get("host_url", "").strip(),
        "discovery_url": request.form.get("discovery_url", "").strip(),
        "account_id": request.form.get("account_id", "").strip(),
        "project_id": request.form.get("project_id", "").strip(),
        "environment_id": request.form.get("environment_id", "").strip(),
        "token": request.form.get("token", "").strip(),
    }

    existing = load_credentials()
    if not data["token"] and existing:
        data["token"] = existing["token"]

    if data["host_url"]:
        data["name"] = data["host_url"].split(".")[0] if "." in data["host_url"] else data["host_url"]

    required = ["host_url", "discovery_url", "account_id", "project_id", "environment_id", "token"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        flash(f"Missing required fields: {', '.join(missing)}", "error")
        return redirect(url_for("setup"))

    from discovery_client import DbtClient
    try:
        client = DbtClient(data)
        client.test_connection()
        save_credentials(data)
        flash("Connection successful! Credentials saved.", "success")
        return redirect(url_for("warehouses"))
    except Exception as e:
        flash(f"Connection failed: {e}", "error")
        save_credentials(data)
        return redirect(url_for("setup"))


@app.route("/setup/clear")
def setup_clear():
    from discovery_client import CREDENTIALS_PATH
    if os.path.exists(CREDENTIALS_PATH):
        os.remove(CREDENTIALS_PATH)
    cache_dir = os.path.join(os.path.dirname(__file__), ".cache")
    if os.path.exists(cache_dir):
        import subprocess
        subprocess.run(["find", cache_dir, "-type", "f", "-delete"], check=False)
        subprocess.run(["find", cache_dir, "-type", "d", "-empty", "-delete"], check=False)
    flash("Credentials cleared.", "success")
    return redirect(url_for("setup"))


@app.route("/warehouses")
def warehouses():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    env_key = _env_key_from_config()
    client = get_client_from_config()
    discovered = discover_warehouses(client)
    wh_config = get_warehouse_config(env_key)
    account_name = creds.get("name", "")

    return render_template(
        "warehouses.html",
        env_key=env_key,
        env_name=account_name,
        warehouses=discovered,
        current_mapping=wh_config["mapping"],
        current_base_cost=wh_config["base_cost"],
        default_base_cost=DEFAULT_BASE_COST,
        sizes=WAREHOUSE_SIZES,
    )


@app.route("/warehouses/save", methods=["POST"])
def save_warehouses():
    env_key = request.form.get("env_key", _env_key_from_config())
    mapping = {}
    for key, value in request.form.items():
        if key.startswith("size_"):
            wh_name = key[5:]
            mapping[wh_name] = value
    base_cost = float(request.form.get("base_cost", DEFAULT_BASE_COST))
    save_warehouse_config(env_key, mapping, base_cost)
    return redirect(url_for("index"))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5555)
    args = parser.parse_args()
    app.run(port=args.port, use_reloader=False)
