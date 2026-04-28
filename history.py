import json
import os
import hashlib
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter
from discovery_client import DbtClient
from cache_db import cache_get as db_get, cache_set as db_set, cache_exists as db_exists

# TTLs — generous so data survives restarts
_AGG_TTL = 6 * 3600       # 6 hours for aggregate results
_API_TTL = 6 * 3600       # 6 hours for individual API responses

# ---------------------------------------------------------------------------
# In-memory aggregate cache (fast path) backed by DuckDB (persistence).
# ---------------------------------------------------------------------------
_AGGREGATE_CACHE = {}
_AGGREGATE_CACHE_LOCK = threading.Lock()


def _agg_db_key(client):
    return f"agg:{client.account_id}:{client.project_id}:{client.environment_id}"


def _agg_cache_get(client):
    key = _agg_db_key(client)
    # Fast path: in-memory
    with _AGGREGATE_CACHE_LOCK:
        entry = _AGGREGATE_CACHE.get(key)
        if entry and (time.time() - entry[0]) < _AGG_TTL:
            return entry[1]
    # Slow path: DuckDB
    data = db_get(key, ttl=_AGG_TTL)
    if data is not None:
        # Restore set type for high_cost_ids
        if "high_cost_ids" in data and isinstance(data["high_cost_ids"], list):
            data["high_cost_ids"] = set(data["high_cost_ids"])
        with _AGGREGATE_CACHE_LOCK:
            _AGGREGATE_CACHE[key] = (time.time(), data)
        return data
    return None


def _agg_cache_set(client, data):
    key = _agg_db_key(client)
    with _AGGREGATE_CACHE_LOCK:
        _AGGREGATE_CACHE[key] = (time.time(), data)
    db_set(key, data)


def is_aggregate_cached(client):
    """Check if aggregate data is available (memory or DuckDB)."""
    key = _agg_db_key(client)
    with _AGGREGATE_CACHE_LOCK:
        entry = _AGGREGATE_CACHE.get(key)
        if entry and (time.time() - entry[0]) < _AGG_TTL:
            return True
    return db_exists(key, ttl=_AGG_TTL)


# ---------------------------------------------------------------------------
# DuckDB cache for individual API responses
# ---------------------------------------------------------------------------


def _cache_key(prefix, *args):
    raw = f"{prefix}:{'|'.join(str(a) for a in args)}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key):
    return db_get(f"api:{key}", ttl=_API_TTL)


def _cache_set(key, data):
    db_set(f"api:{key}", data)


# ---------------------------------------------------------------------------
# Job metadata
# ---------------------------------------------------------------------------


def _get_scheduled_jobs(client: DbtClient):
    """Return dict of job_id -> job info for scheduled jobs."""
    key = _cache_key("scheduled_jobs_v3", client.account_id, client.project_id)
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
                    "execute_steps": job.get("execute_steps") or [],
                }
        offset += 100

    _cache_set(key, scheduled)
    return scheduled


def _cron_to_human(cron_expr):
    """Convert a cron expression to a human-readable cadence string."""
    if not cron_expr:
        return "\u2014"

    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return cron_expr

    minute, hour, dom, month, dow = parts

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

    if hour.startswith("*/"):
        interval = int(hour[2:])
        return f"Every {interval}h, {days}"
    elif "," in hour or "-" in hour:
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
        h = int(hour)
        m = int(minute) if minute != "0" else 0
        time_str = f"{h:02d}:{m:02d}"
        if days == "daily":
            return f"Daily at {time_str}"
        return f"{time_str}, {days}"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def fetch_scheduled_runs(client: DbtClient, days=7):
    """Get all runs from scheduled jobs in the target environment."""
    scheduled_jobs = _get_scheduled_jobs(client)
    print(f"[{client.name}] Found {len(scheduled_jobs)} scheduled jobs")

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
            if str(r.get("environment_id")) != str(client.environment_id):
                continue
            if r["job_definition_id"] not in scheduled_jobs.keys():
                continue
            if r["status"] not in (10, 20):
                continue
            runs.append(r)
        offset += 100

    return runs


def fetch_latest_test_results(client: DbtClient, days=7):
    """Fetch test pass/fail results from the most recent successful run."""
    runs = fetch_scheduled_runs(client, days=days)
    latest = next((r for r in runs if r["status"] == 10), None)
    if not latest:
        return {}

    job_id = latest["job_definition_id"]
    run_id = latest["id"]

    key = _cache_key("run_tests", client.environment_id, job_id, run_id)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    query = """
    query ($jobId: BigInt!, $runId: BigInt) {
      job(id: $jobId, runId: $runId) {
        tests {
          uniqueId
          status
        }
      }
    }
    """
    try:
        data = client.query_discovery(query, variables={"jobId": job_id, "runId": run_id})
        tests = data["job"]["tests"]
        result = {t["uniqueId"]: t["status"] for t in tests}
    except Exception as e:
        print(f"  Warning: Could not fetch test results: {e}")
        result = {}

    _cache_set(key, result)
    return result


def _fetch_single_run_models(client, job_id, run_id):
    """Fetch models for a single run. Returns (run_id, models) or (run_id, None) on error."""
    key = _cache_key("run_models", client.environment_id, job_id, run_id)
    cached = _cache_get(key)
    if cached is not None:
        return run_id, cached

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
          rowsAffected
          stats { id value }
        }
      }
    }
    """
    try:
        data = client.query_discovery(query, variables={"jobId": job_id, "runId": run_id})
        result = data["job"]["models"]
        _cache_set(key, result)
        return run_id, result
    except Exception as e:
        print(f"  Skipping run {run_id}: {e}")
        return run_id, None


def _fetch_all_run_models_parallel(client, runs, max_workers=8):
    """Fetch models for all runs in parallel. Returns dict of run_id -> models list."""
    results = {}
    # Separate cached vs uncached runs
    uncached_runs = []
    for run in runs:
        run_id = run["id"]
        job_id = run["job_definition_id"]
        key = _cache_key("run_models", client.environment_id, job_id, run_id)
        cached = _cache_get(key)
        if cached is not None:
            results[run_id] = cached
        else:
            uncached_runs.append(run)

    if uncached_runs:
        cached_count = len(results)
        total = len(runs)
        print(f"[{client.name}] {cached_count}/{total} runs cached, fetching {len(uncached_runs)} from API ({max_workers} parallel)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _fetch_single_run_models, client, run["job_definition_id"], run["id"]
                ): run["id"]
                for run in uncached_runs
            }
            done_count = 0
            for future in as_completed(futures):
                run_id, models = future.result()
                done_count += 1
                if models is not None:
                    results[run_id] = models
                if done_count % 20 == 0 or done_count == len(uncached_runs):
                    print(f"  Fetched {done_count}/{len(uncached_runs)} runs...")
    else:
        print(f"[{client.name}] All {len(runs)} runs served from cache")

    return results


# ---------------------------------------------------------------------------
# Applied state
# ---------------------------------------------------------------------------


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

        file_path = node.get("filePath") or ""
        path_parts = file_path.split("/")
        if "models" in path_parts:
            idx = len(path_parts) - 1 - path_parts[::-1].index("models")
            path_parts = path_parts[idx + 1:]
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

        freshness = config.get("freshness") or {}
        build_after = freshness.get("build_after") or {}
        details[node["uniqueId"]]["build_after_count"] = build_after.get("count")
        details[node["uniqueId"]]["build_after_period"] = build_after.get("period")
        details[node["uniqueId"]]["updates_on"] = build_after.get("updates_on")
        details[node["uniqueId"]]["has_freshness_config"] = bool(build_after)

    return project_name, details


# ---------------------------------------------------------------------------
# Main aggregate builder
# ---------------------------------------------------------------------------


def build_aggregate(client: DbtClient, days=7, max_models=500):
    """Build aggregated model stats across all runs in the time window.

    Returns (project_name, models, total_runs, high_cost_ids, sao_status, top_project_jobs).
    Also caches the result (and job analysis data) in memory for other tabs.
    """
    # Check in-memory cache first
    cached = _agg_cache_get(client)
    if cached is not None:
        print(f"[{client.name}] Serving aggregate data from in-memory cache")
        c = cached
        return c["project_name"], c["aggregated"], c["total_runs"], c["high_cost_ids"], c["sao_status"], c["top_project_jobs"]

    t0 = time.time()
    runs = fetch_scheduled_runs(client, days=days)
    print(f"[{client.name}] Found {len(runs)} scheduled runs in the past {days} days")

    project_name, applied_details = fetch_applied_model_details(client, max_models=max_models)
    default_warehouse = fetch_warehouse_config(client, client.environment_id)
    scheduled_jobs = _get_scheduled_jobs(client)

    # --- Parallel fetch all run model data ---
    all_run_models = _fetch_all_run_models_parallel(client, runs, max_workers=8)

    # --- Build model history and job_models in a single pass ---
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

    # Also build job -> model set for job analysis (avoids refetching later)
    job_models = defaultdict(set)

    for run in runs:
        run_id = run["id"]
        job_id = run["job_definition_id"]
        models = all_run_models.get(run_id)
        if models is None:
            continue

        for m in models:
            uid = m["uniqueId"]
            mat = m["materializedType"] or "unknown"

            if mat not in ("table", "incremental"):
                continue

            model_status = m["status"]
            if model_status != "success":
                continue

            # Track for job analysis
            job_models[job_id].add(uid)

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
            # Fusion uses "row_count" stat; Core may use "rows_affected" stat
            # or the top-level "rowsAffected" field
            row_count = stats.get("row_count") or stats.get("rows_affected") or stats.get("num_rows")
            if row_count is None and m.get("rowsAffected") is not None:
                row_count = m["rowsAffected"]
            if row_count is not None:
                try:
                    entry["row_counts_by_run"].append((run["created_at"], int(row_count)))
                except (ValueError, TypeError):
                    pass

    print(f"[{client.name}] Processed {len(runs)} runs, found {len(model_history)} table/incremental models with successful builds")

    # Compute project-level top 5 jobs by total runs
    total_runs_per_job = defaultdict(int)
    for run in runs:
        total_runs_per_job[run["job_definition_id"]] += 1
    top_project_jobs = sorted(total_runs_per_job.items(), key=lambda x: x[1], reverse=True)[:5]

    top_project_jobs_info = []
    for jid, total in top_project_jobs:
        jinfo = scheduled_jobs.get(jid, {})
        top_project_jobs_info.append({
            "id": jid,
            "name": jinfo.get("name", f"Job {jid}"),
            "cadence": jinfo.get("cadence", "\u2014"),
            "sao_enabled": jinfo.get("sao_enabled", False),
            "total_runs": total,
        })
    while len(top_project_jobs_info) < 5:
        top_project_jobs_info.append({"id": None, "name": "", "cadence": "", "sao_enabled": False, "total_runs": 0})

    # Compute aggregates
    aggregated = []
    for uid, info in model_history.items():
        if uid not in applied_details:
            continue
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

        daily_counts = Counter()
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
                return "\u2014"
            return scheduled_jobs.get(jid, {}).get("cadence", "\u2014")

        def _job_name(jid):
            if jid is None:
                return ""
            return scheduled_jobs.get(jid, {}).get("name", "")

        def _job_sao(jid):
            if jid is None:
                return None
            return scheduled_jobs.get(jid, {}).get("sao_enabled", False)

        proj_job_runs = []
        for pj_id, _ in top_project_jobs:
            proj_job_runs.append(info["runs_per_job"].get(pj_id, 0))
        while len(proj_job_runs) < 5:
            proj_job_runs.append(0)

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
            "has_description": bool(applied.get("description")),
            "file_path": applied.get("file_path", ""),
            "total_runs": len(info["runs"]),
            "max_daily_runs": max_daily_runs,
            "num_jobs": len(info["job_ids"]),
            "job1_name": _job_name(job1_id),
            "job1_runs": job1_runs,
            "job1_cadence": _job_cadence(job1_id),
            "job1_sao": _job_sao(job1_id),
            "job2_name": _job_name(job2_id),
            "job2_runs": job2_runs,
            "job2_cadence": _job_cadence(job2_id),
            "job2_sao": _job_sao(job2_id),
            "job3_name": _job_name(job3_id),
            "job3_runs": job3_runs,
            "job3_cadence": _job_cadence(job3_id),
            "job3_sao": _job_sao(job3_id),
            "proj_job_runs": proj_job_runs,
            "avg_execution_time": sum(exec_times) / len(exec_times) if exec_times else None,
            "p95_execution_time": sorted(exec_times)[int(len(exec_times) * 0.95)] if exec_times else None,
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
    adjacency = {}
    for uid, detail in applied_details.items():
        adjacency[uid] = detail.get("downstream_tables", [])

    upstream_adjacency = {}
    for uid, detail in applied_details.items():
        upstream_adjacency[uid] = detail.get("upstream_tables", [])

    agg_lookup = {m["unique_id"]: m for m in aggregated}

    def _bfs_downstream(start_uid):
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
        downstream_models = [agg_lookup[uid] for uid in downstream_uids if uid in agg_lookup]

        m["downstream_table_count"] = len(downstream_models)
        m["_downstream_uids"] = [d["unique_id"] for d in downstream_models]
        m["downstream_avg_cost"] = None

        uid = m["unique_id"]
        upstream_uids = upstream_adjacency.get(uid, [])
        upstream_in_agg = [u for u in upstream_uids if u in agg_lookup]
        m["upstream_table_count"] = len(upstream_in_agg)
        m["_upstream_uids"] = upstream_in_agg
        m["upstream_avg_cost"] = None

    elapsed = time.time() - t0
    print(f"[{client.name}] Computed aggregate data for {len(aggregated)} models in {elapsed:.1f}s")

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

    # Build per-job run stats (execution times, daily counts) for jobs optimization
    job_run_stats = defaultdict(lambda: {"durations": [], "daily_counts": Counter()})

    def _parse_hms(val):
        """Parse 'HH:MM:SS' string to seconds."""
        parts = str(val).split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return None

    for run in runs:
        jid = run["job_definition_id"]
        secs = None
        # run_duration and duration are both HH:MM:SS strings in dbt Cloud Admin API
        for field in ("run_duration", "duration"):
            raw = run.get(field)
            if raw and ":" in str(raw):
                try:
                    secs = _parse_hms(raw)
                except (ValueError, IndexError):
                    pass
            if secs is not None:
                break
        if secs is not None:
            job_run_stats[jid]["durations"].append(float(secs))
        day = run.get("created_at", "")[:10]
        if day:
            job_run_stats[jid]["daily_counts"][day] += 1

    # Cache everything — including job_models for the jobs optimization tab
    _agg_cache_set(client, {
        "project_name": project_name,
        "aggregated": aggregated,
        "total_runs": len(runs),
        "high_cost_ids": high_cost_ids,
        "sao_status": sao_status,
        "top_project_jobs": top_project_jobs_info,
        "job_models": {jid: list(uids) for jid, uids in job_models.items()},
        "scheduled_jobs": scheduled_jobs,
        "job_run_stats": {jid: {"durations": s["durations"], "daily_counts": dict(s["daily_counts"])} for jid, s in job_run_stats.items()},
    })

    return project_name, aggregated, len(runs), high_cost_ids, sao_status, top_project_jobs_info


# ---------------------------------------------------------------------------
# Job analysis — derived from cached aggregate data (zero additional API calls)
# ---------------------------------------------------------------------------


def _cron_to_runs_per_day(cron_expr):
    """Rough estimate of runs per day from a cron expression."""
    if not cron_expr:
        return 0
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return 0
    minute, hour, dom, month, dow = parts

    if hour == "*":
        hours = 24
    elif hour.startswith("*/"):
        try:
            hours = 24 / int(hour[2:])
        except (ValueError, ZeroDivisionError):
            hours = 1
    elif "," in hour or "-" in hour:
        h_set = set()
        for p in hour.split(","):
            if "-" in p:
                try:
                    lo, hi = p.split("-")
                    h_set.update(range(int(lo), int(hi) + 1))
                except ValueError:
                    pass
            else:
                try:
                    h_set.add(int(p))
                except ValueError:
                    pass
        hours = len(h_set) or 1
    else:
        hours = 1

    if dow == "*":
        days_ratio = 1.0
    else:
        d_set = set()
        for p in dow.split(","):
            if "-" in p:
                try:
                    lo, hi = p.split("-")
                    d_set.update(range(int(lo), int(hi) + 1))
                except ValueError:
                    pass
            else:
                try:
                    d_set.add(int(p))
                except ValueError:
                    pass
        days_ratio = len(d_set) / 7.0 if d_set else 1.0

    return hours * days_ratio


def _compute_job_analysis(job_models, scheduled_jobs, job_run_stats=None):
    """Compute job overlap analysis from pre-built job -> model set mapping."""
    all_job_ids = [jid for jid in job_models if jid in scheduled_jobs]
    job_run_stats = job_run_stats or {}
    analysis = []

    for jid in all_job_ids:
        info = scheduled_jobs[jid]
        my_models = set(job_models[jid])
        if not my_models:
            continue

        overlaps = []
        all_shared = set()
        my_freq = _cron_to_runs_per_day(info["cron"])

        for other_jid in all_job_ids:
            if other_jid == jid:
                continue
            shared = my_models & set(job_models[other_jid])
            if not shared:
                continue
            all_shared |= shared
            other_info = scheduled_jobs[other_jid]
            other_freq = _cron_to_runs_per_day(other_info["cron"])

            if my_freq > 0 and other_freq > 0:
                ratio = min(my_freq, other_freq) / max(my_freq, other_freq)
                cadence_similar = ratio >= 0.5
                this_less_frequent = my_freq < other_freq * 0.5
            else:
                cadence_similar = False
                this_less_frequent = False

            overlaps.append({
                "job_id": other_jid,
                "job_name": other_info["name"],
                "shared_count": len(shared),
                "shared_pct": round(len(shared) / len(my_models) * 100, 1),
                "other_total": len(job_models[other_jid]),
                "other_cadence": other_info["cadence"],
                "cadence_similar": cadence_similar,
                "this_less_frequent": this_less_frequent,
            })

        overlaps.sort(key=lambda x: x["shared_count"], reverse=True)
        redundancy_pct = round(len(all_shared) / len(my_models) * 100, 1) if my_models else 0

        execute_steps = info.get("execute_steps", [])
        unique_flags = set()
        for step in execute_steps:
            sl = step.lower()
            if "--full-refresh" in sl:
                unique_flags.add("full-refresh")
            if "source freshness" in sl or "source snapshot-freshness" in sl:
                unique_flags.add("source freshness")
            if "dbt seed" in sl:
                unique_flags.add("seed")
            if "dbt snapshot" in sl:
                unique_flags.add("snapshot")
            if "dbt run-operation" in sl:
                unique_flags.add("run-operation")
        unique_flags = sorted(unique_flags)
        has_unique_logic = bool(unique_flags)

        # Job-level execution time stats and max daily runs
        stats = job_run_stats.get(jid, {})
        durations = stats.get("durations", [])
        daily_counts = stats.get("daily_counts", {})
        max_daily_runs = max(daily_counts.values()) if daily_counts else 0
        total_runs = sum(daily_counts.values()) if daily_counts else 0
        avg_duration = sum(durations) / len(durations) if durations else None
        p95_duration = sorted(durations)[int(len(durations) * 0.95)] if durations else None

        analysis.append({
            "job_id": jid,
            "name": info["name"],
            "cron": info["cron"],
            "cadence": info["cadence"],
            "sao_enabled": info["sao_enabled"],
            "execute_steps": execute_steps,
            "runs_per_day": round(my_freq, 1),
            "max_daily_runs": max_daily_runs,
            "total_runs": total_runs,
            "avg_duration": avg_duration,
            "p95_duration": p95_duration,
            "total_models": len(my_models),
            "shared_models": len(all_shared),
            "unique_models": len(my_models) - len(all_shared),
            "redundancy_pct": redundancy_pct,
            "overlaps": overlaps[:5],
            "has_unique_logic": has_unique_logic,
            "unique_flags": unique_flags,
        })

    analysis.sort(key=lambda j: j["redundancy_pct"], reverse=True)
    return analysis


def build_job_analysis(client, days=7):
    """Analyze job overlap and redundancy.

    Uses cached aggregate data when available — zero additional API calls
    if the dashboard or any other tab has already been loaded.
    """
    # Try to use cached data from build_aggregate
    cached = _agg_cache_get(client)
    if cached is not None:
        print(f"[{client.name}] Building job analysis from cached aggregate data (no API calls)")
        job_models = {int(k): v for k, v in cached["job_models"].items()}
        scheduled_jobs = {int(k): v for k, v in cached["scheduled_jobs"].items()}
        jrs = {int(k): v for k, v in cached.get("job_run_stats", {}).items()}
        analysis = _compute_job_analysis(job_models, scheduled_jobs, jrs)
        return cached["project_name"], analysis

    # Cache miss — need to build aggregate first (which caches everything)
    print(f"[{client.name}] No cached data, building full aggregate for job analysis...")
    build_aggregate(client, days=days)

    # Now pull from cache
    cached = _agg_cache_get(client)
    if cached is None:
        return "Unknown", []

    job_models = {int(k): v for k, v in cached["job_models"].items()}
    scheduled_jobs = {int(k): v for k, v in cached["scheduled_jobs"].items()}
    jrs = {int(k): v for k, v in cached.get("job_run_stats", {}).items()}
    analysis = _compute_job_analysis(job_models, scheduled_jobs, jrs)
    return cached["project_name"], analysis
