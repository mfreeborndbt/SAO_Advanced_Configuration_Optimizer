import re
import time
import threading
from discovery_client import DbtClient

# In-memory caches with TTL (same pattern as history.py aggregate cache)
_DETAIL_CACHE = {}
_PK_CACHE = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL = 600  # 10 minutes


_DBT_BUILTINS = frozenset({
    "ref", "source", "config", "this", "var", "env_var", "return", "log",
    "adapter", "exceptions", "set", "is_incremental", "target", "schema",
    "database", "project_name", "run_started_at", "invocation_id",
    "graph", "model", "modules", "flags", "execute", "print",
    "fromjson", "tojson", "fromyaml", "toyaml", "zip", "set_strict",
    "builtins", "dbt_version", "selected_resources",
})


def compute_complexity(raw_code):
    """Derive code complexity signals from raw model code."""
    if not raw_code:
        return {}
    code = raw_code
    lower = code.lower()
    lines = code.strip().split("\n")
    non_blank = [l for l in lines if l.strip()]

    # Detect custom macro calls: Jinja expressions containing function calls
    # that are NOT dbt builtins (ref, source, config, etc.)
    all_calls = re.findall(r'\{[{%][^}%]*?(\w+)\s*\(', code)
    custom_macros = sorted(set(
        name for name in all_calls
        if name.lower() not in _DBT_BUILTINS
    ))

    return {
        "lines": len(lines),
        "lines_non_blank": len(non_blank),
        "joins": len(re.findall(r"\bjoin\b", lower)) - lower.count("cross join"),
        "cross_joins": lower.count("cross join"),
        "ctes": lower.count(" as (") + lower.count(" as("),
        "subqueries": max(0, lower.count("select") - 1),
        "case_stmts": len(re.findall(r"\bcase\b", lower)),
        "window_fns": len(re.findall(r"\bover\s*\(", lower)),
        "group_bys": len(re.findall(r"\bgroup by\b", lower)),
        "unions": len(re.findall(r"\bunion\b", lower)),
        "refs": code.count("ref("),
        "sources": code.count("source("),
        "jinja_blocks": code.count("{%") + code.count("{{"),
        "macros_called": len(re.findall(r"\{\{[^}]*\w+\s*\(", code)),
        "custom_macros": custom_macros,
        "has_custom_macros": len(custom_macros) > 0,
        "if_blocks": code.count("{% if") + code.count("{%- if"),
        "for_loops": code.count("{% for") + code.count("{%- for"),
    }


def fetch_pk_columns_from_tests(client: DbtClient, max_tests=2000):
    """Query unique + not_null tests directly from the Discovery API.
    Results are cached in memory for 10 minutes.

    Returns {model_unique_id: [pk_column_name, ...]} for models where
    the same column has both a unique and a not_null test defined.
    """
    cache_key = str(client.environment_id)
    with _CACHE_LOCK:
        entry = _PK_CACHE.get(cache_key)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            print(f"  PK test data served from cache")
            return entry[1]

    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          tests(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                columnName
                parents { uniqueId }
              }
            }
          }
        }
      }
    }
    """
    all_edges = []
    cursor = None
    try:
        while True:
            variables = {"environmentId": client.environment_id, "first": max_tests}
            if cursor:
                variables["after"] = cursor
            data = client.query_discovery(query, variables=variables)
            tests_data = data["environment"]["applied"]["tests"]
            all_edges.extend(tests_data["edges"])
            if not tests_data["pageInfo"]["hasNextPage"]:
                break
            cursor = tests_data["pageInfo"]["endCursor"]
    except Exception as e:
        print(f"  Warning: Could not fetch tests for PK detection: {e}")
        return {}

    # Build mapping: model_uid -> {col_name: {unique: bool, not_null: bool}}
    model_col_map = {}
    for edge in all_edges:
        node = edge["node"]
        uid = node.get("uniqueId") or ""
        col_name = (node.get("columnName") or "").lower()
        parents = node.get("parents") or []

        # Determine test type from the uniqueId
        parts = uid.split(".")
        test_id = parts[2] if len(parts) >= 3 else ""
        is_unique = test_id.startswith("unique_")
        is_not_null = test_id.startswith("not_null_")
        if not (is_unique or is_not_null):
            continue

        # Find the parent model
        model_uid = None
        for p in parents:
            p_uid = p.get("uniqueId") or ""
            if p_uid.startswith("model.") or p_uid.startswith("snapshot."):
                model_uid = p_uid
                break
        if not model_uid or not col_name:
            continue

        if model_uid not in model_col_map:
            model_col_map[model_uid] = {}
        if col_name not in model_col_map[model_uid]:
            model_col_map[model_uid][col_name] = {"unique": False, "not_null": False}

        if is_unique:
            model_col_map[model_uid][col_name]["unique"] = True
        elif is_not_null:
            model_col_map[model_uid][col_name]["not_null"] = True

    # Keep only columns with BOTH tests
    result = {}
    for model_uid, cols in model_col_map.items():
        pk_cols = sorted(
            col for col, tests in cols.items()
            if tests["unique"] and tests["not_null"]
        )
        if pk_cols:
            result[model_uid] = pk_cols

    with _CACHE_LOCK:
        _PK_CACHE[cache_key] = (time.time(), result)
    return result


def fetch_model_details(client: DbtClient, max_models=500):
    """Fetch full model metadata and code for table/incremental models. Paginates."""
    cache_key = str(client.environment_id)
    with _CACHE_LOCK:
        entry = _DETAIL_CACHE.get(cache_key)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            print(f"  Model details served from cache")
            return entry[1]

    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          models(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                name
                materializedType
                description
                rawCode
                compiledCode
                config
                meta
                language
                access
                contractEnforced
                filePath
                patchPath
                group
                packages
                tags
                fqn
                catalog {
                  columns { name type }
                }
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
    while True:
        variables = {"environmentId": client.environment_id, "first": max_models}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        models_data = data["environment"]["applied"]["models"]
        all_edges.extend(models_data["edges"])
        if not models_data["pageInfo"]["hasNextPage"]:
            break
        cursor = models_data["pageInfo"]["endCursor"]

    models = []
    for edge in all_edges:
        node = edge["node"]
        mat = node["materializedType"] or "unknown"
        if mat not in ("table", "incremental"):
            continue

        config = node.get("config") or {}
        raw_code = node.get("rawCode") or ""
        compiled_code = node.get("compiledCode") or ""
        complexity = compute_complexity(raw_code)

        # Date/timeseries column detection from catalog
        catalog = node.get("catalog") or {}
        col_data = catalog.get("columns") or []
        column_names = [c.get("name", "").lower() for c in col_data]
        date_indicators = ("date", "timestamp", "_at", "created", "updated", "modified")
        has_date_column = any(
            any(ind in cn for ind in date_indicators) for cn in column_names
        )

        # Classify as Date vs Timeseries based on column types
        # Timeseries indicators: timestamp/datetime types, or column names with
        # timestamp/_at/created/updated/modified patterns
        timeseries_type_patterns = ("timestamp", "datetime")
        timeseries_name_patterns = ("timestamp", "_at", "created", "updated", "modified")
        date_type = None
        if has_date_column:
            is_timeseries = False
            for c in col_data:
                cn = (c.get("name") or "").lower()
                ct = (c.get("type") or "").lower()
                if not any(ind in cn for ind in date_indicators):
                    continue
                # Check type first
                if any(tp in ct for tp in timeseries_type_patterns):
                    is_timeseries = True
                    break
                # Check name patterns associated with timeseries
                if any(tp in cn for tp in timeseries_name_patterns):
                    is_timeseries = True
                    break
            date_type = "Timeseries" if is_timeseries else "Date"

        # Window function detection (any analytic OVER clause)
        lower_raw = raw_code.lower()
        has_window_function = bool(re.search(r'\bover\s*\(', lower_raw))

        # Primary key detection: unique_key config, contract, or unique+not_null tests
        materialized = config.get("materialized", mat)
        unique_key = config.get("unique_key")
        contract_enforced = node.get("contractEnforced") or False

        # Check for unique + not_null tests on the same column (= primary key)
        children = node.get("children") or []
        test_uids = [c["uniqueId"] for c in children if c.get("resourceType") == "test"]
        model_name = node["name"]
        unique_test_map = {}   # col -> test_uid
        not_null_test_map = {}  # col -> test_uid
        for uid in test_uids:
            parts = uid.split(".")
            if len(parts) >= 3:
                test_part = parts[2]
                if test_part.startswith(f"unique_{model_name}_"):
                    col = test_part[len(f"unique_{model_name}_"):]
                    unique_test_map[col] = uid
                elif test_part.startswith(f"not_null_{model_name}_"):
                    col = test_part[len(f"not_null_{model_name}_"):]
                    not_null_test_map[col] = uid
        pk_columns_from_tests = set(unique_test_map.keys()) & set(not_null_test_map.keys())
        pk_test_uids = {
            col: {"unique": unique_test_map[col], "not_null": not_null_test_map[col]}
            for col in pk_columns_from_tests
        }

        # pk_values: column name contains standalone "id" (not "lid", "idle", etc.)
        pk_value_cols = [cn for cn in column_names if re.search(r'(^|_)id(_|$)', cn)]

        has_potential_pk = bool(unique_key) or contract_enforced or bool(pk_columns_from_tests) or bool(pk_value_cols)
        strategy = config.get("incremental_strategy")
        pre_hooks = config.get("pre_hook") or []
        post_hooks = config.get("post_hook") or []
        grants = config.get("grants") or {}
        tags = node.get("tags") or config.get("tags") or []
        table_format = config.get("table_format")
        external_volume = config.get("external_volume")

        models.append({
            "unique_id": node["uniqueId"],
            "name": node["name"],
            "materialized": mat,
            "file_path": node.get("filePath") or "",
            "language": node.get("language") or "sql",
            "access": node.get("access") or "protected",
            "contract_enforced": node.get("contractEnforced") or False,
            "group": node.get("group"),
            "description": node.get("description") or "",
            "has_description": bool((node.get("description") or "").strip()),
            "tags": tags,
            "fqn": node.get("fqn") or [],
            "layer": (node.get("fqn") or ["", ""])[1] if len(node.get("fqn") or []) > 1 else "",
            "meta": node.get("meta") or {},
            "patch_path": node.get("patchPath"),
            "has_yaml": bool(node.get("patchPath")),
            "packages": node.get("packages") or [],
            "unique_key": unique_key,
            "incremental_strategy": strategy,
            "has_pre_hooks": len(pre_hooks) > 0,
            "has_post_hooks": len(post_hooks) > 0,
            "pre_hook_count": len(pre_hooks),
            "post_hook_count": len(post_hooks),
            "grants": grants,
            "has_grants": bool(grants),
            "table_format": table_format,
            "external_volume": external_volume,
            "column_names": column_names,
            "has_date_column": has_date_column,
            "date_type": date_type,
            "has_window_function": has_window_function,
            "has_potential_pk": has_potential_pk,
            "contract_enforced": contract_enforced,
            "pk_test_uids": pk_test_uids,
            "pk_columns_from_tests": sorted(pk_columns_from_tests),
            "pk_value_cols": pk_value_cols,
            "raw_code": raw_code,
            "compiled_code_len": len(compiled_code),
            "raw_code_len": len(raw_code),
            **complexity,
        })

    models.sort(key=lambda m: m.get("lines", 0), reverse=True)

    with _CACHE_LOCK:
        _DETAIL_CACHE[cache_key] = (time.time(), models)
    return models
