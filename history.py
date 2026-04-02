import json
import os
import hashlib
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from discovery_client import DbtClient

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".cache")
CACHE_TTL = 600  # 10 minutes


def _cache_key(prefix, *args):
    raw = f"{prefix}:{'|'.join(str(a) for a in args)}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if os.path.exists(path):
        age = time.time() - os.path.getmtime(path)
        if age < CACHE_TTL:
            with open(path) as f:
                return json.load(f)
    return None


def _cache_set(key, data):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    with open(path, "w") as f:
        json.dump(data, f)


def _get_scheduled_jobs(client: DbtClient):
    """Return dict of job_id -> job info for scheduled jobs."""
    key = _cache_key("scheduled_jobs_v2", client.account_id, client.project_id)
    cached = _cache_get(key)
    if cached is not None:
        return {int(k): v for k, v in cached.items()}

    scheduled = {}
    offset = 0
    while True:
        data = client.admin_get("jobs/", params={
            "project_id": client.project_id,
            "offset": offset,
            "limit": 100,
        })
        batch = data["data"]
        if not batch:
            break
        for job in batch:
            triggers = job.get("triggers") or {}
            if triggers.get("schedule"):
                cron = (job.get("schedule") or {}).get("cron", "")
                cost_features = job.get("cost_optimization_features") or []
                scheduled[job["id"]] = {
                    "name": job.get("name", ""),
                    "cron": cron,
                    "cadence": _cron_to_human(cron),
                    "sao_enabled": "state_aware_orchestration" in cost_features,
                    "compare_changes_flags": job.get("compare_changes_flags", ""),
                }
        offset += 100

    _cache_set(key, scheduled)
    return scheduled


def _cron_to_human(cron_expr):
    """Convert a cron expression to a human-readable cadence string."""
    if not cron_expr:
        return "—"

    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return cron_expr

    minute, hour, dom, month, dow = parts

    # Day names
    day_names = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}

    def _parse_dow(dow_str):
        if dow_str == "*":
            return "daily"
        try:
            nums = []
            for part in dow_str.split(","):
                if "-" in part:
                    lo, hi = part.split("-")
                    nums.extend(range(int(lo), int(hi) + 1))
                else:
                    nums.append(int(part))
            nums = sorted(set(nums))
            if nums == list(range(7)) or nums == list(range(0, 7)):
                return "daily"
            if nums == [1, 2, 3, 4, 5]:
                return "weekdays"
            if set(nums) == {0, 1, 2, 3, 4, 5}:
                return "Sun-Fri"
            return ", ".join(day_names.get(n, str(n)) for n in nums)
        except ValueError:
            return dow_str

    days = _parse_dow(dow)

    # Frequency
    if hour.startswith("*/"):
        interval = int(hour[2:])
        return f"Every {interval}h, {days}"
    elif "," in hour or "-" in hour:
        # Count distinct hours
        hours = set()
        for part in hour.split(","):
            if "-" in part:
                lo, hi = part.split("-")
                hours.update(range(int(lo), int(hi) + 1))
            else:
                hours.add(int(part))
        count = len(hours)
        lo, hi = min(hours), max(hours)
        return f"{count}x/day ({lo:02d}-{hi:02d}h), {days}"
    elif hour == "*":
        return f"Hourly, {days}"
    else:
        # Specific hour
        h = int(hour)
        m = int(minute) if minute != "0" else 0
        time_str = f"{h:02d}:{m:02d}"
        if days == "daily":
            return f"Daily at {time_str}"
        return f"{time_str}, {days}"


def fetch_scheduled_runs(client: DbtClient, days=7):
    """Get all runs from scheduled jobs in the target environment.

    Includes both successful and errored runs, since errored runs
    often contain many models that built successfully.
    """
    scheduled_jobs = _get_scheduled_jobs(client)
    print(f"[{client.name}] Found {len(scheduled_jobs)} scheduled jobs")

    # Use start of day N days ago to avoid cutting off early-morning runs
    now = datetime.now(timezone.utc)
    cutoff_date = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0)
    cutoff = cutoff_date.isoformat()[:19]
    runs = []
    offset = 0

    while True:
        params = {
            "project_id": client.project_id,
            "order_by": "-created_at",
            "limit": 100,
            "offset": offset,
        }
        data = client.admin_get("runs/", params=params)
        batch = data["data"]
        if not batch:
            break
        for r in batch:
            if r["created_at"][:19] < cutoff:
                return runs
            # Must be in the target environment
            if str(r.get("environment_id")) != str(client.environment_id):
                continue
            # Must be from a scheduled job
            if r["job_definition_id"] not in scheduled_jobs.keys():
                continue
            # Must be finished (success or error, not running/queued/cancelled)
            if r["status"] not in (10, 20):
                continue
            runs.append(r)
        offset += 100

    return runs


def fetch_run_models_with_stats(client: DbtClient, job_id, run_id):
    """Get per-model execution data + stats for a specific run. Cached per run."""
    key = _cache_key("run_models", client.environment_id, job_id, run_id)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    query = """
    query ($jobId: BigInt!, $runId: BigInt) {
      job(id: $jobId, runId: $runId) {
        models {
          uniqueId
          name
          schema
          database
          executionTime
          materializedType
          status
          executeCompletedAt
          stats { id value }
        }
      }
    }
    """
    data = client.query_discovery(query, variables={"jobId": job_id, "runId": run_id})
    result = data["job"]["models"]
    _cache_set(key, result)
    return result


def fetch_warehouse_config(client: DbtClient, environment_id):
    """Get the default warehouse from the environment's connection config."""
    try:
        data = client.admin_get_v3(f"environments/{environment_id}/")
        env = data["data"]
        conn_details = (
            env.get("project", {}).get("connection", {}).get("details", {})
        )
        return conn_details.get("warehouse")
    except Exception:
        return None


def fetch_applied_model_details(client: DbtClient, max_models=500):
    """Get lineage, layer, language, warehouse config from applied state. Paginates."""
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        dbtProjectName
        applied {
          models(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                materializedType
                language
                filePath
                fqn
                tags
                config
                catalog {
                  columns { name }
                }
                parents { uniqueId }
                ancestors(types: [Model, Source, Seed]) { uniqueId }
                children { uniqueId resourceType }
              }
            }
          }
        }
      }
    }
    """
    all_edges = []
    cursor = None
    project_name = ""

    while True:
        variables = {"environmentId": client.environment_id, "first": max_models}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        env = data["environment"]
        project_name = env["dbtProjectName"]
        models_data = env["applied"]["models"]
        all_edges.extend(models_data["edges"])
        if not models_data["pageInfo"]["hasNextPage"]:
            break
        cursor = models_data["pageInfo"]["endCursor"]

    print(f"[{client.name}] Fetched {len(all_edges)} total models from applied state")

    mat_lookup = {}
    details = {}
    for edge in all_edges:
        node = edge["node"]
        mat_lookup[node["uniqueId"]] = node["materializedType"] or "unknown"

    for edge in all_edges:
        node = edge["node"]
        catalog = node.get("catalog") or {}
        columns = catalog.get("columns") or []
        children = node.get("children") or []
        fqn = node.get("fqn") or []
        config = node.get("config") or {}

        downstream_materialized = [
            c["uniqueId"]
            for c in children
            if c["resourceType"] == "model"
            and mat_lookup.get(c["uniqueId"]) in ("table", "incremental")
        ]

        parents = node.get("parents") or []
        upstream_materialized = [
            p["uniqueId"]
            for p in parents
            if mat_lookup.get(p["uniqueId"]) in ("table", "incremental")
        ]

        # Parse folder levels from filePath
        file_path = node.get("filePath") or ""
        path_parts = file_path.split("/")
        # Strip leading models/ or dbt_packages/.../models/
        if "models" in path_parts:
            idx = len(path_parts) - 1 - path_parts[::-1].index("models")
            path_parts = path_parts[idx + 1:]
        # Remove the filename
        if path_parts and "." in path_parts[-1]:
            path_parts = path_parts[:-1]
        folder1 = path_parts[0] if len(path_parts) > 0 else ""
        folder2 = path_parts[1] if len(path_parts) > 1 else ""
        folder3 = path_parts[2] if len(path_parts) > 2 else ""

        details[node["uniqueId"]] = {
            "column_count": len(columns),
            "direct_dep_count": len(node.get("parents") or []),
            "total_dep_count": len(node.get("ancestors") or []),
            "downstream_tables": downstream_materialized,
            "upstream_tables": upstream_materialized,
            "language": node.get("language") or "sql",
            "layer": fqn[1] if len(fqn) > 1 else "",
            "folder1": folder1,
            "folder2": folder2,
            "folder3": folder3,
            "tags": node.get("tags") or [],
            "snowflake_warehouse": config.get("snowflake_warehouse"),
        }

        # Extract freshness / build_after / updates_on config
        freshness = config.get("freshness") or {}
        build_after = freshness.get("build_after") or {}
        details[node["uniqueId"]]["build_after_count"] = build_after.get("count")
        details[node["uniqueId"]]["build_after_period"] = build_after.get("period")
        details[node["uniqueId"]]["updates_on"] = build_after.get("updates_on")
        details[node["uniqueId"]]["has_freshness_config"] = bool(build_after)

    return project_name, details


def build_aggregate(client: DbtClient, days=7, max_models=500):
    """Build aggregated model stats across all runs in the time window."""
    runs = fetch_scheduled_runs(client, days=days)
    print(f"[{client.name}] Found {len(runs)} scheduled runs in the past {days} days")

    project_name, applied_details = fetch_applied_model_details(client, max_models=max_models)
    default_warehouse = fetch_warehouse_config(client, client.environment_id)
    scheduled_jobs = _get_scheduled_jobs(client)

    model_history = defaultdict(lambda: {
        "name": "",
        "schema": "",
        "database": "",
        "materialized": "",
        "runs": [],
        "job_ids": set(),
        "runs_per_job": defaultdict(int),
        "row_counts_by_run": [],
    })

    for i, run in enumerate(runs):
        run_id = run["id"]
        job_id = run["job_definition_id"]

        try:
            models = fetch_run_models_with_stats(client, job_id, run_id)
        except Exception as e:
            print(f"  Skipping run {run_id}: {e}")
            continue

        for m in models:
            uid = m["uniqueId"]
            mat = m["materializedType"] or "unknown"

            if mat not in ("table", "incremental"):
                continue

            # Only track models that actually ran (not reused/skipped)
            model_status = m["status"]
            if model_status != "success":
                continue

            entry = model_history[uid]
            entry["name"] = m["name"]
            entry["schema"] = m["schema"]
            entry["database"] = m["database"]
            entry["materialized"] = mat
            entry["job_ids"].add(job_id)
            entry["runs_per_job"][job_id] += 1
            entry["runs"].append({
                "run_id": run_id,
                "job_id": job_id,
                "run_at": run["created_at"],
                "execution_time": m["executionTime"],
                "status": model_status,
            })

            stats = {s["id"]: s["value"] for s in (m.get("stats") or [])}
            row_count = stats.get("row_count")
            if row_count is not None:
                entry["row_counts_by_run"].append((run["created_at"], int(row_count)))

    print(f"[{client.name}] Processed {len(runs)} runs, found {len(model_history)} table/incremental models with successful builds")

    # Compute aggregates — only for models that still exist in applied state
    aggregated = []
    for uid, info in model_history.items():
        if uid not in applied_details:
            continue  # model was removed from the project
        exec_times = [r["execution_time"] for r in info["runs"] if r["execution_time"] is not None]
        applied = applied_details[uid]
        col_count = applied.get("column_count", 0)

        row_counts = sorted(info["row_counts_by_run"], key=lambda x: x[0])
        earliest_rows = row_counts[0][1] if row_counts else None
        latest_rows = row_counts[-1][1] if row_counts else None
        row_delta = (latest_rows - earliest_rows) if (latest_rows is not None and earliest_rows is not None) else None

        runs_with_change = 0
        runs_with_data = len(row_counts)
        for i in range(1, len(row_counts)):
            if row_counts[i][1] != row_counts[i - 1][1]:
                runs_with_change += 1
        change_pct = (runs_with_change / (runs_with_data - 1) * 100) if runs_with_data > 1 else None
        total_runs_count = len(info["runs"])
        avg_new_rows = (row_delta / total_runs_count) if (row_delta is not None and total_runs_count > 0) else None

        # Max scheduled daily runs: group runs by date, find the peak day
        from collections import Counter as _Counter
        daily_counts = _Counter()
        for r in info["runs"]:
            day = r["run_at"][:10]
            daily_counts[day] += 1
        max_daily_runs = max(daily_counts.values()) if daily_counts else 0

        sorted_jobs = sorted(info["runs_per_job"].items(), key=lambda x: x[1], reverse=True)
        job1_id = sorted_jobs[0][0] if len(sorted_jobs) > 0 else None
        job1_runs = sorted_jobs[0][1] if len(sorted_jobs) > 0 else 0
        job2_id = sorted_jobs[1][0] if len(sorted_jobs) > 1 else None
        job2_runs = sorted_jobs[1][1] if len(sorted_jobs) > 1 else 0
        job3_id = sorted_jobs[2][0] if len(sorted_jobs) > 2 else None
        job3_runs = sorted_jobs[2][1] if len(sorted_jobs) > 2 else 0

        def _job_cadence(jid):
            if jid is None:
                return "—"
            return scheduled_jobs.get(jid, {}).get("cadence", "—")

        model_wh = applied.get("snowflake_warehouse")
        warehouse = model_wh or default_warehouse

        aggregated.append({
            "unique_id": uid,
            "name": info["name"],
            "schema": info["schema"],
            "database": info["database"],
            "materialized": info["materialized"],
            "language": applied.get("language", "sql"),
            "layer": applied.get("layer", ""),
            "folder1": applied.get("folder1", ""),
            "folder2": applied.get("folder2", ""),
            "folder3": applied.get("folder3", ""),
            "tags": applied.get("tags", []),
            "warehouse": warehouse,
            "warehouse_override": model_wh is not None,
            "build_after_count": applied.get("build_after_count"),
            "build_after_period": applied.get("build_after_period"),
            "updates_on": applied.get("updates_on"),
            "has_freshness_config": applied.get("has_freshness_config", False),
            "total_runs": len(info["runs"]),
            "max_daily_runs": max_daily_runs,
            "num_jobs": len(info["job_ids"]),
            "job1_runs": job1_runs,
            "job1_cadence": _job_cadence(job1_id),
            "job2_runs": job2_runs,
            "job2_cadence": _job_cadence(job2_id),
            "job3_runs": job3_runs,
            "job3_cadence": _job_cadence(job3_id),
            "avg_execution_time": sum(exec_times) / len(exec_times) if exec_times else None,
            "max_execution_time": max(exec_times) if exec_times else None,
            "min_execution_time": min(exec_times) if exec_times else None,
            "total_execution_time": sum(exec_times) if exec_times else None,
            "earliest_rows": earliest_rows,
            "latest_rows": latest_rows,
            "row_delta": row_delta,
            "runs_with_change": runs_with_change,
            "runs_with_data": runs_with_data,
            "change_pct": change_pct,
            "avg_new_rows": avg_new_rows,
            "column_count": col_count,
            "total_values": (latest_rows * col_count) if latest_rows is not None else None,
        })

    # --- Downstream dependency graph traversal ---
    # Build adjacency list: model_uid -> [direct downstream table/incremental uids]
    adjacency = {}
    for uid, detail in applied_details.items():
        adjacency[uid] = detail.get("downstream_tables", [])

    # Build upstream adjacency list: model_uid -> [direct upstream table/incremental uids]
    upstream_adjacency = {}
    for uid, detail in applied_details.items():
        upstream_adjacency[uid] = detail.get("upstream_tables", [])

    # Build lookup of uid -> aggregated model data for cost/time
    agg_lookup = {m["unique_id"]: m for m in aggregated}

    def _bfs_downstream(start_uid):
        """BFS to find all transitive downstream table/incremental models."""
        visited = set()
        queue = list(adjacency.get(start_uid, []))
        while queue:
            uid = queue.pop(0)
            if uid in visited or uid == start_uid:
                continue
            visited.add(uid)
            queue.extend(adjacency.get(uid, []))
        return visited

    for m in aggregated:
        downstream_uids = _bfs_downstream(m["unique_id"])
        # Filter to only models that exist in our aggregated set (current, with runs)
        downstream_models = [agg_lookup[uid] for uid in downstream_uids if uid in agg_lookup]

        m["downstream_table_count"] = len(downstream_models)
        m["_downstream_uids"] = [d["unique_id"] for d in downstream_models]
        m["downstream_avg_cost"] = None  # computed after cost mapping in app.py

        # Upstream: direct parent table/incremental models only (no BFS)
        uid = m["unique_id"]
        upstream_uids = upstream_adjacency.get(uid, [])
        upstream_in_agg = [u for u in upstream_uids if u in agg_lookup]
        m["upstream_table_count"] = len(upstream_in_agg)
        m["_upstream_uids"] = upstream_in_agg
        m["upstream_avg_cost"] = None  # computed after cost mapping in app.py

    print(f"[{client.name}] Computed downstream dependency costs for {len(aggregated)} models")

    aggregated.sort(key=lambda m: m["avg_execution_time"] or 0, reverse=True)

    # Compute top-20% thresholds
    all_total = sorted([m["total_execution_time"] for m in aggregated if m["total_execution_time"] is not None])
    all_avg = sorted([m["avg_execution_time"] for m in aggregated if m["avg_execution_time"] is not None])
    p80_total = all_total[int(len(all_total) * 0.8)] if all_total else 0
    p80_avg = all_avg[int(len(all_avg) * 0.8)] if all_avg else 0

    high_cost_ids = set()
    for m in aggregated:
        if (m["total_execution_time"] or 0) >= p80_total or (m["avg_execution_time"] or 0) >= p80_avg:
            high_cost_ids.add(m["unique_id"])

    # Build SAO status summary
    enabled_jobs = [{"id": jid, "name": info["name"]} for jid, info in scheduled_jobs.items() if info.get("sao_enabled")]
    disabled_jobs = [{"id": jid, "name": info["name"]} for jid, info in scheduled_jobs.items() if not info.get("sao_enabled")]
    sao_status = {
        "any_enabled": len(enabled_jobs) > 0,
        "all_enabled": len(disabled_jobs) == 0 and len(enabled_jobs) > 0,
        "enabled_jobs": enabled_jobs,
        "disabled_jobs": disabled_jobs,
    }

    return project_name, aggregated, len(runs), high_cost_ids, sao_status
