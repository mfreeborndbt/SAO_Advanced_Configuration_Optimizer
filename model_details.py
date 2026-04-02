import re
from discovery_client import DbtClient


def compute_complexity(raw_code):
    """Derive code complexity signals from raw model code."""
    if not raw_code:
        return {}
    code = raw_code
    lower = code.lower()
    lines = code.strip().split("\n")
    non_blank = [l for l in lines if l.strip()]

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
        "if_blocks": code.count("{% if") + code.count("{%- if"),
        "for_loops": code.count("{% for") + code.count("{%- for"),
    }


def fetch_model_details(client: DbtClient, max_models=500):
    """Fetch full model metadata and code for table/incremental models. Paginates."""
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

        materialized = config.get("materialized", mat)
        unique_key = config.get("unique_key")
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
            "raw_code": raw_code,
            "compiled_code_len": len(compiled_code),
            "raw_code_len": len(raw_code),
            **complexity,
        })

    models.sort(key=lambda m: m.get("lines", 0), reverse=True)
    return models
