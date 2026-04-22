import json
import os
import queue
import re
import sys
import threading
from flask import Flask, render_template, request, redirect, url_for, flash, Response
from discovery_client import load_credentials, save_credentials, get_client_from_config
from history import build_aggregate, build_job_analysis, fetch_latest_test_results, is_aggregate_cached
from model_details import fetch_model_details, fetch_pk_columns_from_tests
from warehouse_config import (
    discover_warehouses,
    get_warehouse_config,
    save_warehouse_config,
    calculate_model_costs,
    WAREHOUSE_SIZES,
    DEFAULT_BASE_COST,
)

app = Flask(__name__)
app.secret_key = "dbt-observability-setup-key"


# ---------------------------------------------------------------------------
# Log capture for streaming loading progress to the browser
# ---------------------------------------------------------------------------

class LogTee:
    """Tee stdout to both the original stream and a queue for SSE streaming.

    Intentionally does NOT inherit from io.TextIOBase — its internal RLock
    causes deadlocks when ThreadPoolExecutor workers all print simultaneously.
    """
    def __init__(self, original, q):
        self.original = original
        self.queue = q

    def write(self, text):
        self.original.write(text)
        stripped = text.rstrip()
        if stripped:
            self.queue.put(stripped)
        return len(text)

    def flush(self):
        self.original.flush()

    # Delegate everything else to the original stream so libraries
    # that inspect stdout (isatty, fileno, encoding, etc.) keep working.
    def __getattr__(self, name):
        return getattr(self.original, name)


def _preload_page(page):
    """Run all data loading for a page, warming caches."""
    creds = load_credentials()
    if creds is None:
        raise Exception("Not configured")
    client = get_client_from_config()

    if page in ("/", "/models-optimization", "/updated-at", "/build-after"):
        _load_models_with_costs()
    if page == "/models-optimization":
        print("Fetching model details...")
        fetch_model_details(client)
        print("Fetching test results...")
        fetch_latest_test_results(client, days=7)
        print("Fetching primary key tests...")
        fetch_pk_columns_from_tests(client)
        print("Models optimization data ready.")
    elif page == "/jobs-optimization":
        build_job_analysis(client, days=7)


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


@app.errorhandler(500)
def handle_500(e):
    flash(f"Something went wrong: {e}. Try again or check your credentials.", "error")
    return redirect(url_for("setup"))


class ApiAuthError(Exception):
    """Raised when API calls fail due to authentication issues."""
    pass


def _load_models_with_costs():
    """Shared data loader for dashboard and optimization tabs.

    Returns (project_name, models, total_runs, has_costs, env_key, account_name, sao_status)
    or None if not configured.
    Raises ApiAuthError if the API returns 401/403.
    """
    creds = load_credentials()
    if creds is None:
        return None

    env_key = _env_key_from_config()
    client = get_client_from_config()

    try:
        project_name, models, total_runs, high_cost_ids, sao_status, top_project_jobs = build_aggregate(
            client, days=7, max_models=500,
        )
    except Exception as e:
        err = str(e)
        if "401" in err or "403" in err or "Unauthorized" in err or "Forbidden" in err:
            raise ApiAuthError(err)
        if "429" in err or "Too Many Requests" in err:
            raise ApiAuthError("Rate limited by dbt Cloud API. Please wait a moment and try again.")
        raise

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
    return project_name, models, total_runs, has_costs, env_key, account_name, sao_status, top_project_jobs


def _needs_loading(page):
    """Check if we should redirect to the loading page (cache is cold)."""
    creds = load_credentials()
    if creds is None:
        return False  # will redirect to setup
    client = get_client_from_config()
    if client is None:
        return False
    return not is_aggregate_cached(client)


@app.route("/loading")
def loading():
    next_page = request.args.get("next", "/")
    creds = load_credentials()
    project_name = creds.get("name", "Project") if creds else "Project"
    # Map page paths to display names
    page_names = {
        "/": "Full Model Context",
        "/models-optimization": "Models Optimization",
        "/jobs-optimization": "Jobs Optimization",
        "/updated-at": "Updated At",
        "/build-after": "Build After",
    }
    page_name = page_names.get(next_page, next_page)
    return render_template("loading.html", next_page=next_page, page_name=page_name, project_name=project_name)


@app.route("/api/load")
def api_load():
    page = request.args.get("page", "/")

    def generate():
        q = queue.Queue()
        result = {"status": "done", "error": None}

        def do_load():
            old_stdout = sys.stdout
            sys.stdout = LogTee(old_stdout, q)
            try:
                _preload_page(page)
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
            finally:
                sys.stdout = old_stdout
                q.put(None)  # sentinel

        thread = threading.Thread(target=do_load, daemon=True)
        thread.start()

        while True:
            try:
                msg = q.get(timeout=0.5)
                if msg is None:
                    if result["status"] == "done":
                        yield f"data: {json.dumps({'type': 'done', 'redirect': page})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'error', 'message': result['error']})}\n\n"
                    break
                yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
            except queue.Empty:
                yield ": heartbeat\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/")
def index():
    if _needs_loading("/"):
        return redirect(url_for("loading", next="/"))
    try:
        result = _load_models_with_costs()
    except ApiAuthError:
        flash("API authentication failed. Please update your service token.", "error")
        return redirect(url_for("setup"))
    if result is None:
        return redirect(url_for("setup"))

    project_name, models, total_runs, has_costs, env_key, account_name, sao_status, top_project_jobs = result

    return render_template(
        "index.html",
        project_name=project_name,
        env_name=account_name,
        env_key=env_key,
        models=models,
        total_runs=total_runs,
        has_costs=has_costs,
        project_jobs=top_project_jobs,
        active_tab="dashboard",
    )


@app.route("/data-dictionary")
def data_dictionary():
    creds = load_credentials()
    project_name = creds.get("name", "Project") if creds else "Project"
    return render_template(
        "data_dictionary.html",
        project_name=project_name,
        active_tab="data_dictionary",
    )


@app.route("/models-optimization")
def models_optimization():
    if _needs_loading("/models-optimization"):
        return redirect(url_for("loading", next="/models-optimization"))
    try:
        result = _load_models_with_costs()
    except ApiAuthError:
        flash("API authentication failed. Please update your service token.", "error")
        return redirect(url_for("setup"))
    if result is None:
        return redirect(url_for("setup"))

    project_name, models, total_runs, has_costs, env_key, account_name, sao_status, top_project_jobs = result

    # Fetch code-level details, test results, and PK tests for enrichment
    client = get_client_from_config()
    detail_models = fetch_model_details(client)
    details_lookup = {m["unique_id"]: m for m in detail_models}
    test_results = fetch_latest_test_results(client, days=7)
    pk_cols_from_api = fetch_pk_columns_from_tests(client)

    # Compute top 20% expense threshold (per run)
    if has_costs:
        expense_vals = sorted([m["avg_cost"] for m in models if m.get("avg_cost")])
    else:
        expense_vals = sorted([m["avg_execution_time"] for m in models if m.get("avg_execution_time")])
    p80 = expense_vals[int(len(expense_vals) * 0.8)] if expense_vals else 0

    # Filter models for optimization candidates
    candidates = []
    for m in models:
        if m["materialized"] != "table":
            continue

        details = details_lookup.get(m["unique_id"], {})

        rows = m.get("latest_rows") or 0
        avg_time = m.get("avg_execution_time") or 0
        expense = m.get("avg_cost") or avg_time if has_costs else avg_time

        size_time_qualifies = rows >= 1_000_000 and avg_time >= 60
        top20_qualifies = expense >= p80

        if not (size_time_qualifies or top20_qualifies):
            continue

        has_pk = details.get("has_potential_pk", bool(details.get("unique_key")))
        has_date = details.get("has_date_column", False)
        if not (has_pk or has_date):
            continue

        # Merge aggregated + detail data
        enriched = dict(m)
        for k, v in details.items():
            if k not in enriched:
                enriched[k] = v
        enriched["has_window_function"] = details.get("has_window_function", False)
        enriched["window_fns"] = details.get("window_fns", 0)

        # PK detection columns
        # pk_test: unique + not_null tests on same column, both PASSED on last run
        pk_test_uids = details.get("pk_test_uids", {})
        pk_test = False
        pk_test_cols = []
        for col, uids in pk_test_uids.items():
            u_status = test_results.get(uids["unique"], "")
            n_status = test_results.get(uids["not_null"], "")
            if u_status == "pass" and n_status == "pass":
                pk_test = True
                pk_test_cols.append(col)
        enriched["pk_test"] = pk_test
        enriched["pk_test_cols"] = pk_test_cols

        # pk_values: standalone "id" column detected
        enriched["pk_values"] = bool(details.get("pk_value_cols"))
        enriched["pk_value_col_names"] = details.get("pk_value_cols", [])

        # likely_pass_pk: either or both are true
        enriched["likely_pass_pk"] = pk_test or enriched["pk_values"]

        # Consolidated primary key column.
        # Priority: (1) Direct API test query (most reliable — same source as dbt Cloud PK badge)
        #           (2) Children-based test parsing (fallback)
        #           (3) unique_key from config
        api_pk_cols = pk_cols_from_api.get(m["unique_id"], [])
        children_pk_cols = details.get("pk_columns_from_tests", [])
        if api_pk_cols:
            enriched["primary_key_cols"] = api_pk_cols
            enriched["pk_verified"] = True
        elif children_pk_cols:
            enriched["primary_key_cols"] = list(children_pk_cols)
            enriched["pk_verified"] = bool(pk_test_cols)
        elif details.get("unique_key"):
            uk = details["unique_key"]
            enriched["primary_key_cols"] = uk if isinstance(uk, list) else [uk]
            enriched["pk_verified"] = False
        else:
            enriched["primary_key_cols"] = []
            enriched["pk_verified"] = False

        # Percent of new rows per run
        avg_new = enriched.get("avg_new_rows")
        latest = enriched.get("latest_rows")
        if avg_new is not None and latest and latest > 0:
            enriched["new_rows_pct"] = round(avg_new / latest * 100, 2)
        else:
            enriched["new_rows_pct"] = None

        qualifications = []
        if size_time_qualifies:
            qualifications.append(f"{rows:,} rows, {fmt_duration(avg_time)} avg")
        if top20_qualifies:
            qualifications.append("Top 20% expensive")
        enriched["qualification"] = " | ".join(qualifications)

        candidates.append(enriched)

    candidates.sort(key=lambda m: m.get("avg_execution_time") or 0, reverse=True)

    return render_template(
        "models_optimization.html",
        project_name=project_name,
        env_name=account_name,
        models=candidates,
        all_model_count=len(models),
        total_runs=total_runs,
        has_costs=has_costs,
        active_tab="models_optimization",
    )


@app.route("/jobs-optimization")
def jobs_optimization():
    if _needs_loading("/jobs-optimization"):
        return redirect(url_for("loading", next="/jobs-optimization"))
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    client = get_client_from_config()
    account_name = creds.get("name", "")
    try:
        project_name, jobs = build_job_analysis(client, days=7)
    except Exception as e:
        err = str(e)
        if any(s in err for s in ("401", "403", "429", "Unauthorized", "Forbidden", "Too Many")):
            msg = "Rate limited — please wait a moment and retry." if "429" in err or "Too Many" in err else "API authentication failed. Please update your service token."
            flash(msg, "error")
            return redirect(url_for("setup"))
        raise

    return render_template(
        "jobs_optimization.html",
        project_name=project_name,
        env_name=account_name,
        jobs=jobs,
        active_tab="jobs_optimization",
    )


@app.route("/setup")
def setup():
    creds = load_credentials()
    return render_template("setup.html", creds=creds)


@app.route("/setup/save", methods=["POST"])
def setup_save():
    prefix = request.form.get("account_prefix", "").strip().lower()
    # Strip any accidentally pasted full URL down to just the prefix
    prefix = prefix.replace("https://", "").replace("http://", "").split(".")[0]
    region = request.form.get("region", "us1").strip().lower()

    data = {
        "account_prefix": prefix,
        "region": region,
        "host_url": f"{prefix}.{region}.dbt.com" if prefix else "",
        "discovery_url": f"https://{prefix}.metadata.{region}.dbt.com/graphql" if prefix else "",
        "account_id": request.form.get("account_id", "").strip(),
        "project_id": request.form.get("project_id", "").strip(),
        "environment_id": request.form.get("environment_id", "").strip(),
        "token": request.form.get("token", "").strip(),
    }

    existing = load_credentials()
    if not data["token"] and existing:
        data["token"] = existing["token"]

    if prefix:
        data["name"] = prefix

    required = ["account_prefix", "account_id", "project_id", "environment_id", "token"]
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
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)
    flash("Credentials cleared.", "success")
    return redirect(url_for("setup"))


@app.route("/warehouses")
def warehouses():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    env_key = _env_key_from_config()
    client = get_client_from_config()
    try:
        discovered = discover_warehouses(client)
    except Exception as e:
        if "401" in str(e) or "403" in str(e) or "Unauthorized" in str(e) or "Forbidden" in str(e):
            flash("API authentication failed. Please update your service token.", "error")
            return redirect(url_for("setup"))
        raise
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


@app.route("/updated-at")
def updated_at():
    if _needs_loading("/updated-at"):
        return redirect(url_for("loading", next="/updated-at"))
    try:
        result = _load_models_with_costs()
    except ApiAuthError:
        flash("API authentication failed. Please update your service token.", "error")
        return redirect(url_for("setup"))
    if result is None:
        return redirect(url_for("setup"))

    project_name, models, total_runs, has_costs, env_key, account_name, sao_status, top_project_jobs = result

    # Build lookup for static detection: a model is "static" if it never changed rows
    model_lookup = {m["unique_id"]: m for m in models}
    for m in models:
        m["is_static"] = (
            m.get("runs_with_data", 0) > 1
            and (m.get("change_pct") == 0 or m.get("change_pct") is None)
            and (m.get("row_delta") or 0) == 0
        )

    # For each model, check how many upstream tables are static
    for m in models:
        upstream_uids = m.get("_upstream_uids", [])
        static_upstream = [
            uid for uid in upstream_uids
            if uid in model_lookup and model_lookup[uid].get("is_static", False)
        ]
        m["static_upstream_count"] = len(static_upstream)
        m["has_static_upstream"] = len(static_upstream) > 0

    # Filter to incremental models (SAO updated_at applies to incremental)
    # Include table models too since they could be converted
    candidates = []
    for m in models:
        if m["materialized"] not in ("table", "incremental"):
            continue
        upstream_count = m.get("upstream_table_count", 0)
        candidates.append(m)

    candidates.sort(key=lambda m: m.get("downstream_avg_cost") or 0, reverse=True)

    return render_template(
        "updated_at.html",
        project_name=project_name,
        env_name=account_name,
        models=candidates,
        all_model_count=len(models),
        total_runs=total_runs,
        has_costs=has_costs,
        active_tab="updated_at",
    )


@app.route("/build-after")
def build_after():
    if _needs_loading("/build-after"):
        return redirect(url_for("loading", next="/build-after"))
    try:
        result = _load_models_with_costs()
    except ApiAuthError:
        flash("API authentication failed. Please update your service token.", "error")
        return redirect(url_for("setup"))
    if result is None:
        return redirect(url_for("setup"))

    project_name, models, total_runs, has_costs, env_key, account_name, sao_status, top_project_jobs = result

    # Filter to table/incremental models with run data
    candidates = []
    for m in models:
        if m["materialized"] not in ("table", "incremental"):
            continue
        if not m.get("total_runs"):
            continue

        # Compute "potential skip %" — % of runs that had NO row changes
        # This approximates how often SAO could skip this model
        candidates.append(m)

    candidates.sort(key=lambda m: m.get("avg_cost") or m.get("avg_execution_time") or 0, reverse=True)

    return render_template(
        "build_after.html",
        project_name=project_name,
        env_name=account_name,
        models=candidates,
        all_model_count=len(models),
        total_runs=total_runs,
        has_costs=has_costs,
        active_tab="build_after",
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
    app.run(port=args.port, use_reloader=False, threaded=True)
