"""SAO Optimization Recommendation Engine.

Analyzes aggregated model data and generates actionable SAO configuration
recommendations. Prefers layer/folder-level rules over per-model configs.
"""

from collections import defaultdict


def _percentile(values, pct):
    """Compute the pct-th percentile from a list of numbers."""
    if not values:
        return 0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


def _fmt_cost(val):
    if val is None:
        return "N/A"
    if val < 1:
        return f"${val:.2f}"
    return f"${val:,.2f}"


def _fmt_duration(seconds):
    if seconds is None:
        return "N/A"
    s = float(seconds)
    if s < 60:
        return f"{s:.0f}s"
    elif s < 3600:
        return f"{int(s // 60)}m {int(s % 60)}s"
    else:
        return f"{int(s // 3600)}h {int((s % 3600) // 60)}m"


def _group_by_folder(models):
    """Group models by (layer, folder1). Returns dict of key -> [models]."""
    groups = defaultdict(list)
    for m in models:
        key = (m.get("layer", ""), m.get("folder1", ""))
        groups[key] = groups.get(key, [])
        groups[key].append(m)
    return groups


def _folder_path(layer, folder1):
    """Build a human-readable folder path."""
    parts = [p for p in [layer, folder1] if p]
    return "/".join(parts) if parts else "(root)"


def _make_yaml_folder(path, config_lines):
    """Generate a dbt_project.yml YAML snippet for folder-level config."""
    parts = path.split("/")
    indent = "  "
    lines = ["models:"]
    prefix = "  "
    for part in parts:
        lines.append(f"{prefix}{part}:")
        prefix += "  "
    for line in config_lines:
        lines.append(f"{prefix}{line}")
    return "\n".join(lines)


def _make_yaml_models(model_names, config_lines):
    """Generate YAML snippet for individual model configs."""
    lines = []
    for name in model_names[:5]:
        lines.append(f"# In models/{name}.yml or dbt_project.yml:")
        lines.append(f"models:")
        lines.append(f"  - name: {name}")
        lines.append(f"    config:")
        for cl in config_lines:
            lines.append(f"      {cl}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Category 1: Job Overlap
# ---------------------------------------------------------------------------
def _detect_job_overlap(models, has_costs):
    overlap = [m for m in models if m.get("num_jobs", 0) >= 2]
    if not overlap:
        return []

    overlap.sort(key=lambda m: m.get("total_cost") or m.get("total_execution_time") or 0, reverse=True)

    total_duplicate_cost = 0
    total_duplicate_runs = 0
    affected = []
    for m in overlap:
        extra_runs = m["total_runs"] - m["job1_runs"]
        dup_cost = extra_runs * (m.get("avg_cost") or 0) if has_costs else None
        total_duplicate_runs += extra_runs
        if dup_cost:
            total_duplicate_cost += dup_cost
        affected.append({
            "name": m["name"],
            "unique_id": m["unique_id"],
            "avg_cost": m.get("avg_cost"),
            "total_runs": m["total_runs"],
            "total_cost": m.get("total_cost"),
            "num_jobs": m["num_jobs"],
        })

    savings = total_duplicate_cost if has_costs and total_duplicate_cost > 0 else None
    pct = (total_duplicate_runs / sum(m["total_runs"] for m in overlap) * 100) if overlap else 0

    return [{
        "id": "cat1-job-overlap",
        "category": "job_overlap",
        "category_name": "Job Overlap",
        "color": "orange",
        "priority": "high",
        "scope": "model",
        "scope_path": "",
        "title": f"{len(overlap)} models built in multiple jobs — {total_duplicate_runs} duplicate runs/week",
        "description": (
            f"These models are built by more than one scheduled job, causing redundant rebuilds. "
            f"Assign single job ownership per model using dbt selection syntax to eliminate duplicates."
        ),
        "affected_models": affected,
        "affected_count": len(affected),
        "suggested_config": {
            "level": "job",
            "yaml": (
                "# For each job, use dbt selection syntax to assign ownership:\n"
                "# Job A: dbt build --select tag:job_a\n"
                "# Job B: dbt build --select tag:job_b\n"
                "#\n"
                "# Or use --exclude to remove overlapping models from secondary jobs."
            ),
        },
        "estimated_impact": {
            "weekly_savings": savings,
            "weekly_savings_formatted": _fmt_cost(savings),
            "runs_eliminated": total_duplicate_runs,
            "pct_reduction": round(pct, 1),
            "description": f"~{_fmt_cost(savings)}/week by eliminating {total_duplicate_runs} duplicate runs" if savings else f"~{total_duplicate_runs} duplicate runs/week eliminated",
        },
        "sort_key": savings if savings else total_duplicate_runs,
    }]


# ---------------------------------------------------------------------------
# Category 2: Expensive High-Frequency Models
# ---------------------------------------------------------------------------
def _already_has_build_after(m, max_hours):
    """Return True if model already has build_after <= max_hours (already throttled)."""
    count = m.get("build_after_count")
    period = m.get("build_after_period")
    if not count or not period:
        return False
    period_hours = {"minute": 1/60, "hour": 1, "day": 24}.get(period, 0)
    return (count * period_hours) <= max_hours


def _detect_expensive_high_frequency(models, has_costs):
    if has_costs:
        costs = [m["avg_cost"] for m in models if m.get("avg_cost")]
        cost_p75 = _percentile(costs, 75)
    else:
        times = [m["avg_execution_time"] for m in models if m.get("avg_execution_time")]
        cost_p75 = _percentile(times, 75)

    runs_list = [m["total_runs"] for m in models if m.get("total_runs")]
    runs_p75 = _percentile(runs_list, 75)

    def qualifies(m):
        # Skip if already throttled to <= 1 hour
        if _already_has_build_after(m, max_hours=1):
            return False
        if has_costs:
            expensive = (m.get("avg_cost") or 0) >= cost_p75
        else:
            expensive = (m.get("avg_execution_time") or 0) >= cost_p75
        frequent = (m.get("total_runs") or 0) >= runs_p75 or (m.get("max_daily_runs") or 0) >= 3
        return expensive and frequent

    qualified = [m for m in models if qualifies(m)]
    if not qualified:
        return []

    groups = _group_by_folder(qualified)
    all_in_folder = _group_by_folder(models)
    recs = []

    for key, group_models in groups.items():
        layer, folder1 = key
        path = _folder_path(layer, folder1)
        total_in_folder = len(all_in_folder.get(key, []))
        ratio = len(group_models) / total_in_folder if total_in_folder else 0

        if ratio >= 0.6 and len(group_models) >= 2:
            savings = sum(m.get("total_cost") or 0 for m in group_models) * 0.5 if has_costs else None
            runs_saved = sum(m["total_runs"] for m in group_models) * 0.5

            affected = [{"name": m["name"], "unique_id": m["unique_id"],
                         "avg_cost": m.get("avg_cost"), "total_runs": m["total_runs"],
                         "total_cost": m.get("total_cost")} for m in group_models]

            recs.append({
                "id": f"cat2-expensive-freq-{layer}-{folder1}",
                "category": "expensive_high_frequency",
                "category_name": "Expensive High-Frequency",
                "color": "red",
                "priority": "high",
                "scope": "folder",
                "scope_path": path,
                "title": f"Throttle {len(group_models)} expensive models in {path}",
                "description": (
                    f"{len(group_models)} of {total_in_folder} models in {path} are both expensive and frequently built. "
                    f"Adding a build_after interval reduces unnecessary rebuilds while maintaining freshness."
                ),
                "affected_models": affected,
                "affected_count": len(affected),
                "suggested_config": {
                    "level": "folder",
                    "yaml": _make_yaml_folder(path, ["+freshness:", "  build_after:", "    count: 1", "    period: hour"]),
                },
                "estimated_impact": {
                    "weekly_savings": savings,
                    "weekly_savings_formatted": _fmt_cost(savings),
                    "runs_eliminated": int(runs_saved),
                    "pct_reduction": 50.0,
                    "description": f"~{_fmt_cost(savings)}/week by reducing ~{int(runs_saved)} rebuilds" if savings else f"~{int(runs_saved)} fewer rebuilds/week",
                },
                "sort_key": savings if savings else runs_saved,
            })

    return recs


# ---------------------------------------------------------------------------
# Category 3: Folder-Level updates_on Policy
# ---------------------------------------------------------------------------
def _detect_folder_updates_on(models, has_costs):
    groups = _group_by_folder(models)
    recs = []

    for key, group_models in groups.items():
        if len(group_models) < 3:
            continue

        layer, folder1 = key
        path = _folder_path(layer, folder1)

        low_change = [m for m in group_models
                      if m.get("updates_on") != "all"  # skip models already configured
                      and ((m.get("change_pct") is not None and m["change_pct"] < 10)
                           or (m.get("change_pct") is None and m.get("runs_with_change", 0) == 0
                               and m.get("runs_with_data", 0) >= 3))]

        if len(low_change) / len(group_models) < 0.6:
            continue

        savings = 0
        runs_saved = 0
        for m in low_change:
            cp = m.get("change_pct")
            if cp is not None and cp == 0:
                factor = 0.8
            else:
                factor = 0.5
            runs_saved += m["total_runs"] * factor
            if has_costs and m.get("total_cost"):
                savings += m["total_cost"] * factor

        affected = [{"name": m["name"], "unique_id": m["unique_id"],
                     "avg_cost": m.get("avg_cost"), "total_runs": m["total_runs"],
                     "total_cost": m.get("total_cost"),
                     "change_pct": m.get("change_pct")} for m in low_change]

        recs.append({
            "id": f"cat3-updates-on-{layer}-{folder1}",
            "category": "folder_updates_on",
            "category_name": "Set updates_on: all",
            "color": "blue",
            "priority": "medium",
            "scope": "folder",
            "scope_path": path,
            "title": f"Set updates_on: all for {path} ({len(low_change)}/{len(group_models)} models rarely change)",
            "description": (
                f"{len(low_change)} of {len(group_models)} models in {path} had <10% row change rate. "
                f"Setting updates_on: all means models only rebuild when ALL upstream dependencies have new data, "
                f"reducing unnecessary cascading rebuilds."
            ),
            "affected_models": affected,
            "affected_count": len(affected),
            "suggested_config": {
                "level": "folder",
                "yaml": _make_yaml_folder(path, ["+freshness:", "  build_after:", "    updates_on: all"]),
            },
            "estimated_impact": {
                "weekly_savings": savings if has_costs and savings > 0 else None,
                "weekly_savings_formatted": _fmt_cost(savings) if has_costs and savings > 0 else "N/A",
                "runs_eliminated": int(runs_saved),
                "pct_reduction": round(runs_saved / max(sum(m["total_runs"] for m in low_change), 1) * 100, 1),
                "description": f"~{_fmt_cost(savings)}/week, ~{int(runs_saved)} fewer rebuilds" if has_costs and savings > 0 else f"~{int(runs_saved)} fewer rebuilds/week",
            },
            "sort_key": savings if has_costs and savings > 0 else runs_saved,
        })

    return recs


# ---------------------------------------------------------------------------
# Category 4: Downstream Cost Amplifiers
# ---------------------------------------------------------------------------
def _detect_downstream_amplifiers(models, has_costs):
    downstream_costs = [m["downstream_avg_cost"] for m in models if m.get("downstream_avg_cost")]
    downstream_counts = [m["downstream_table_count"] for m in models if m.get("downstream_table_count")]

    if not downstream_costs and not downstream_counts:
        return []

    cost_p80 = _percentile(downstream_costs, 80) if downstream_costs else 0
    count_p80 = _percentile(downstream_counts, 80) if downstream_counts else 0

    qualified = []
    for m in models:
        dc = m.get("downstream_avg_cost") or 0
        dt = m.get("downstream_table_count") or 0
        cp = m.get("change_pct")
        if (dc >= cost_p80 or dt >= count_p80) and dt >= 5:
            if cp is None or cp < 50:
                qualified.append(m)

    qualified.sort(key=lambda m: m.get("downstream_avg_cost") or 0, reverse=True)
    recs = []

    for m in qualified[:10]:
        cp = m.get("change_pct")
        unnecessary = (100 - cp) / 100 if cp is not None else 0.5
        wasted = (m.get("downstream_avg_cost") or 0) * m["total_runs"] * unnecessary if has_costs else None
        runs_impact = int(m["total_runs"] * unnecessary)

        recs.append({
            "id": f"cat4-downstream-{m['name']}",
            "category": "downstream_amplifier",
            "category_name": "Downstream Cost Amplifier",
            "color": "red",
            "priority": "high",
            "scope": "model",
            "scope_path": m["name"],
            "title": f"{m['name']} triggers {m['downstream_table_count']} downstream tables ({_fmt_cost(m.get('downstream_avg_cost'))}/run)",
            "description": (
                f"Rebuilding {m['name']} cascades to {m['downstream_table_count']} downstream table/incremental models "
                f"costing {_fmt_cost(m.get('downstream_avg_cost'))} per run. "
                f"With only {cp or '~50'}% of runs showing data changes, many cascades are unnecessary. "
                f"Adding updates_on: all and a build_after buffer reduces wasted downstream work."
            ),
            "affected_models": [{"name": m["name"], "unique_id": m["unique_id"],
                                 "avg_cost": m.get("avg_cost"), "total_runs": m["total_runs"],
                                 "total_cost": m.get("total_cost"),
                                 "downstream_table_count": m["downstream_table_count"],
                                 "downstream_avg_cost": m.get("downstream_avg_cost")}],
            "affected_count": 1,
            "suggested_config": {
                "level": "model",
                "yaml": _make_yaml_models([m["name"]], ["+freshness:", "  build_after:", "    count: 2", "    period: hour", "    updates_on: all"]),
            },
            "estimated_impact": {
                "weekly_savings": wasted,
                "weekly_savings_formatted": _fmt_cost(wasted),
                "runs_eliminated": runs_impact,
                "pct_reduction": round(unnecessary * 100, 1),
                "description": f"~{_fmt_cost(wasted)}/week in avoided downstream cascades" if wasted else f"~{runs_impact} unnecessary cascade triggers/week",
            },
            "sort_key": wasted if wasted else runs_impact,
        })

    return recs


# ---------------------------------------------------------------------------
# Category 5: Static Tables
# ---------------------------------------------------------------------------
def _detect_static_tables(models, has_costs):
    static = []
    for m in models:
        cp = m.get("change_pct")
        rwc = m.get("runs_with_change", 0)
        rwd = m.get("runs_with_data", 0)
        tr = m.get("total_runs", 0)
        if tr < 5:
            continue
        # Skip if already at 1 day or longer build_after
        if _already_has_build_after(m, max_hours=24):
            continue
        if cp == 0 or (cp is None and rwc == 0 and rwd >= 3):
            static.append(m)

    if not static:
        return []

    groups = _group_by_folder(static)
    all_in_folder = _group_by_folder(models)
    recs = []

    for key, group_models in groups.items():
        layer, folder1 = key
        path = _folder_path(layer, folder1)
        total_in_folder = len(all_in_folder.get(key, []))
        ratio = len(group_models) / total_in_folder if total_in_folder else 0

        savings = sum(m.get("total_cost") or 0 for m in group_models) if has_costs else None
        runs_saved = sum(m["total_runs"] for m in group_models)

        affected = [{"name": m["name"], "unique_id": m["unique_id"],
                     "avg_cost": m.get("avg_cost"), "total_runs": m["total_runs"],
                     "total_cost": m.get("total_cost")} for m in group_models]

        if ratio >= 0.7 and len(group_models) >= 2:
            recs.append({
                "id": f"cat5-static-{layer}-{folder1}",
                "category": "static_tables",
                "category_name": "Static Tables",
                "color": "green",
                "priority": "medium",
                "scope": "folder",
                "scope_path": path,
                "title": f"{len(group_models)} static models in {path} — data never changed",
                "description": (
                    f"{len(group_models)} of {total_in_folder} models in {path} showed zero row changes across all runs. "
                    f"These models are being rebuilt unnecessarily. Set a daily build_after to dramatically reduce frequency."
                ),
                "affected_models": affected,
                "affected_count": len(affected),
                "suggested_config": {
                    "level": "folder",
                    "yaml": _make_yaml_folder(path, ["+freshness:", "  build_after:", "    count: 1", "    period: day"]),
                },
                "estimated_impact": {
                    "weekly_savings": savings if has_costs and savings > 0 else None,
                    "weekly_savings_formatted": _fmt_cost(savings) if has_costs and savings > 0 else "N/A",
                    "runs_eliminated": runs_saved,
                    "pct_reduction": 90.0,
                    "description": f"~{_fmt_cost(savings)}/week — all {runs_saved} rebuilds are wasted" if savings and savings > 0 else f"~{runs_saved} wasted rebuilds/week",
                },
                "sort_key": savings if savings and savings > 0 else runs_saved,
            })
        elif len(group_models) <= 5:
            for m in group_models:
                ms = m.get("total_cost") or 0
                recs.append({
                    "id": f"cat5-static-model-{m['name']}",
                    "category": "static_tables",
                    "category_name": "Static Table",
                    "color": "green",
                    "priority": "low",
                    "scope": "model",
                    "scope_path": m["name"],
                    "title": f"{m['name']} is static — {m['total_runs']} unnecessary rebuilds",
                    "description": f"Row count never changed across {m['total_runs']} runs. Consider a daily build_after.",
                    "affected_models": [{"name": m["name"], "unique_id": m["unique_id"],
                                         "avg_cost": m.get("avg_cost"), "total_runs": m["total_runs"],
                                         "total_cost": m.get("total_cost")}],
                    "affected_count": 1,
                    "suggested_config": {
                        "level": "model",
                        "yaml": _make_yaml_models([m["name"]], ["+freshness:", "  build_after:", "    count: 1", "    period: day"]),
                    },
                    "estimated_impact": {
                        "weekly_savings": ms if has_costs and ms > 0 else None,
                        "weekly_savings_formatted": _fmt_cost(ms) if has_costs and ms > 0 else "N/A",
                        "runs_eliminated": m["total_runs"],
                        "pct_reduction": 90.0,
                        "description": f"~{_fmt_cost(ms)}/week" if has_costs and ms > 0 else f"~{m['total_runs']} wasted rebuilds",
                    },
                    "sort_key": ms if has_costs and ms > 0 else m["total_runs"],
                })

    return recs


# ---------------------------------------------------------------------------
# Category 6: Upstream Trigger Sensitivity
# ---------------------------------------------------------------------------
def _detect_upstream_sensitivity(models, has_costs):
    qualified = [m for m in models
                 if (m.get("upstream_table_count") or 0) >= 5
                 and m.get("updates_on") != "all"  # skip already configured
                 and (m.get("change_pct") is None or m["change_pct"] < 30)]

    if not qualified:
        return []

    groups = _group_by_folder(qualified)
    all_in_folder = _group_by_folder(models)
    recs = []

    for key, group_models in groups.items():
        layer, folder1 = key
        path = _folder_path(layer, folder1)
        total_in_folder = len(all_in_folder.get(key, []))
        ratio = len(group_models) / total_in_folder if total_in_folder else 0

        if ratio < 0.5 or len(group_models) < 2:
            continue

        savings = sum((m.get("total_cost") or 0) * 0.6 for m in group_models) if has_costs else None
        runs_saved = sum(m["total_runs"] * 0.6 for m in group_models)

        affected = [{"name": m["name"], "unique_id": m["unique_id"],
                     "avg_cost": m.get("avg_cost"), "total_runs": m["total_runs"],
                     "total_cost": m.get("total_cost"),
                     "upstream_table_count": m.get("upstream_table_count")} for m in group_models]

        recs.append({
            "id": f"cat6-upstream-{layer}-{folder1}",
            "category": "upstream_sensitivity",
            "category_name": "Upstream Trigger Sensitivity",
            "color": "blue",
            "priority": "medium",
            "scope": "folder",
            "scope_path": path,
            "title": f"{len(group_models)} models in {path} have 5+ upstream tables — switch to updates_on: all",
            "description": (
                f"These models have many upstream table dependencies. With the default updates_on: any, "
                f"a change in ANY upstream triggers a rebuild. Switching to updates_on: all means they only "
                f"rebuild when ALL upstreams have new data, significantly reducing trigger frequency."
            ),
            "affected_models": affected,
            "affected_count": len(affected),
            "suggested_config": {
                "level": "folder",
                "yaml": _make_yaml_folder(path, ["+freshness:", "  build_after:", "    updates_on: all"]),
            },
            "estimated_impact": {
                "weekly_savings": savings if has_costs and savings > 0 else None,
                "weekly_savings_formatted": _fmt_cost(savings) if has_costs and savings > 0 else "N/A",
                "runs_eliminated": int(runs_saved),
                "pct_reduction": 60.0,
                "description": f"~{_fmt_cost(savings)}/week, ~{int(runs_saved)} fewer trigger events" if has_costs and savings > 0 else f"~{int(runs_saved)} fewer trigger events/week",
            },
            "sort_key": savings if has_costs and savings > 0 else runs_saved,
        })

    return recs


# ---------------------------------------------------------------------------
# Priority assignment + main entry point
# ---------------------------------------------------------------------------
def _assign_priorities(recs):
    """Assign high/medium/low based on relative impact ranking."""
    if not recs:
        return recs
    recs.sort(key=lambda r: r["sort_key"] or 0, reverse=True)
    n = len(recs)
    for i, r in enumerate(recs):
        pct = i / n
        if pct < 0.2:
            r["priority"] = "high"
        elif pct < 0.5:
            r["priority"] = "medium"
        else:
            r["priority"] = "low"
    return recs


def _detect_sao_not_enabled(sao_status):
    """Generate a recommendation if SAO is not enabled on any job."""
    if sao_status and sao_status.get("any_enabled"):
        return []

    return [{
        "id": "cat0-sao-not-enabled",
        "category": "sao_setup",
        "category_name": "Enable SAO",
        "color": "red",
        "priority": "high",
        "scope": "project",
        "scope_path": "",
        "title": "State-Aware Orchestration is not enabled on any scheduled job",
        "description": (
            "SAO (State-Aware Orchestration) is not enabled on any of your scheduled production jobs. "
            "SAO automatically skips rebuilding models when upstream data hasn't changed, which is the "
            "foundation for all other optimization recommendations. Enable SAO on your jobs first, "
            "let it run for at least one week to collect baseline data, then re-run this analysis "
            "for targeted configuration recommendations."
        ),
        "affected_models": [],
        "affected_count": 0,
        "suggested_config": {
            "level": "job",
            "yaml": (
                "# In dbt Cloud:\n"
                "# 1. Go to Deploy > Jobs > [Your Job] > Settings\n"
                "# 2. Under 'Execution Settings', enable 'State-Aware Orchestration'\n"
                "# 3. Repeat for each scheduled production job\n"
                "#\n"
                "# Or via API: Add 'state_aware_orchestration' to cost_optimization_features"
            ),
        },
        "estimated_impact": {
            "weekly_savings": None,
            "weekly_savings_formatted": "Significant (20%+ typical)",
            "runs_eliminated": 0,
            "pct_reduction": 0,
            "description": "Enable SAO first — typical savings are 20%+ on compute costs",
        },
        "sort_key": 999999,
    }]


_MAINTENANCE_KEYWORDS = ("maintenance", "snapshot", "schema", "ci ", "[ci]", "clone", "drop", "adhoc", "ad-hoc", "admin", "alert")


def _is_model_building_job(job):
    """Heuristic: skip maintenance/CI jobs when recommending SAO expansion."""
    name = (job.get("name") or "").lower()
    return not any(kw in name for kw in _MAINTENANCE_KEYWORDS)


def _detect_partial_sao(sao_status):
    """Recommend enabling SAO on production scheduled jobs that don't have it yet."""
    if not sao_status or not sao_status.get("any_enabled"):
        return []
    if sao_status.get("all_enabled"):
        return []

    # Only flag model-building jobs, not maintenance/schema/CI jobs
    disabled = [j for j in sao_status.get("disabled_jobs", []) if _is_model_building_job(j)]
    if not disabled:
        return []

    job_names = [j["name"] for j in disabled]
    return [{
        "id": "cat0-sao-partial",
        "category": "sao_setup",
        "category_name": "Expand SAO",
        "color": "orange",
        "priority": "high",
        "scope": "job",
        "scope_path": "",
        "title": f"Enable SAO on {len(disabled)} remaining scheduled job{'s' if len(disabled) > 1 else ''}",
        "description": (
            f"SAO is enabled on some jobs but not all. These scheduled jobs are still running full rebuilds: "
            f"{', '.join(job_names)}. Enabling SAO on all scheduled jobs ensures consistent skip behavior "
            f"and prevents redundant work across the pipeline."
        ),
        "affected_models": [{"name": j["name"], "unique_id": str(j["id"])} for j in disabled],
        "affected_count": len(disabled),
        "suggested_config": {
            "level": "job",
            "yaml": "# Enable SAO on these jobs:\n" + "\n".join(f"# - {j['name']} (Job ID: {j['id']})" for j in disabled),
        },
        "estimated_impact": {
            "weekly_savings": None,
            "weekly_savings_formatted": "N/A",
            "runs_eliminated": 0,
            "pct_reduction": 0,
            "description": f"Enable SAO on {len(disabled)} job{'s' if len(disabled) > 1 else ''} for full coverage",
        },
        "sort_key": 99999,
    }]


def _fmt_build_after(m):
    """Format a model's current build_after as a readable string."""
    count = m.get("build_after_count")
    period = m.get("build_after_period")
    uo = m.get("updates_on")
    if not count and not period:
        return None
    parts = []
    if count and period:
        parts.append(f"{count} {period}")
    if uo:
        parts.append(f"updates_on: {uo}")
    return ", ".join(parts)


def generate_recommendations(models, has_costs, sao_status=None):
    """Generate SAO optimization recommendations from aggregated model data.

    Args:
        models: list of model dicts from build_aggregate + calculate_model_costs
        has_costs: bool indicating whether dollar cost data is available
        sao_status: dict with SAO enablement info per job

    Returns:
        list of recommendation dicts, sorted by estimated impact descending
    """
    if not models:
        return []

    recs = []

    # Check SAO enablement first
    if sao_status and not sao_status.get("any_enabled"):
        return _detect_sao_not_enabled(sao_status)

    # Partial SAO coverage
    recs.extend(_detect_partial_sao(sao_status))

    # Core recommendations — filter out models that already have the recommended config
    recs.extend(_detect_job_overlap(models, has_costs))
    recs.extend(_detect_expensive_high_frequency(models, has_costs))
    recs.extend(_detect_folder_updates_on(models, has_costs))
    recs.extend(_detect_downstream_amplifiers(models, has_costs))
    recs.extend(_detect_static_tables(models, has_costs))
    recs.extend(_detect_upstream_sensitivity(models, has_costs))

    recs = _assign_priorities(recs)
    return recs[:10]
