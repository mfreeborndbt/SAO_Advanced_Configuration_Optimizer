# SAO Advanced Configuration Optimizer

A local observability dashboard for dbt Cloud that surfaces model-level execution data and generates actionable SAO (State-Aware Orchestration) configuration recommendations — helping teams reduce compute costs by 20%+ through smarter scheduling policies.

## What it does

- **Dashboard** — per-model execution stats, costs, row changes, upstream/downstream dependency costs, and schedule cadence across all scheduled production jobs
- **Recommendations** — automated SAO tuning suggestions (updates_on, build_after, job overlap detection) with YAML snippets ready to paste into `dbt_project.yml`
- **Cost modeling** — maps your Snowflake/Databricks warehouse sizes to a per-second cost rate, then calculates per-model and downstream spend

## Quick start

**Requirements:** Python 3.8+, a dbt Cloud account with read + metadata access token **(Admin API access may be needed, still testing this out)**

```bash
git clone https://github.com/mfreeborndbt/SAO_Advanced_Configuration_Optimizer.git
cd SAO_Advanced_Configuration_Optimizer
./run.sh
```

That's it. The script:
1. Creates a local virtual environment
2. Installs dependencies
3. Opens `http://localhost:5555` in your browser
4. Walks you through entering your dbt Cloud credentials

> **Windows users:** Run `python app.py` after `pip install -r requirements.txt`

## Setup flow

1. **API credentials** — Enter your dbt Cloud account prefix, Account ID, Project ID, Production Environment ID, and a service token with read + metadata access
2. **Warehouse costs** — Map each warehouse name to a Snowflake/Databricks size tier (XS → 4XL) and set your base cost per second
3. **Dashboard** — Explore model stats, filter, and sort. Then check the Recommendations tab for tuning suggestions

## Finding your credentials

| Field | Where to find it |
|---|---|
| Account Prefix | The subdomain of your dbt Cloud URL: `abc123` from `abc123.us1.dbt.com` |
| Account ID | dbt Cloud URL: `/deploy/{account_id}/...` |
| Project ID | dbt Cloud URL: `/projects/{project_id}/...` |
| Environment ID | Your production deployment environment ID |
| Service Token | Account Settings → Service Tokens → create with **read** + **metadata** access |

## Security

- Credentials are stored locally in `config/credentials.json` (gitignored, never leaves your machine)
- The service token is masked in the UI after first entry
- No data is sent anywhere except to your dbt Cloud instance

## Stopping the app

`Ctrl+C` in the terminal where `run.sh` is running.

## What SAO is

State-Aware Orchestration is a dbt Cloud Enterprise feature that automatically skips rebuilding models when upstream data hasn't changed. This tool helps you tune SAO's `updates_on` and `build_after` settings at the layer, folder, or model level — the configurations that determine *how aggressively* SAO skips work.
