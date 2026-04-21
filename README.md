# SAO Advanced Configuration Optimizer

A local dashboard that connects to your dbt Cloud account and generates actionable recommendations for tuning [State-Aware Orchestration](https://docs.getdbt.com/docs/deploy/deploy-jobs#state-aware-orchestration) — helping teams reduce compute costs through smarter scheduling.

## Getting started

**You need:** Python 3.8+ and a dbt Cloud service token with read + metadata access.

### macOS / Linux

```bash
git clone https://github.com/mfreeborndbt/SAO_Advanced_Configuration_Optimizer.git
cd SAO_Advanced_Configuration_Optimizer
./run.sh
```

### Windows

```powershell
git clone https://github.com/mfreeborndbt/SAO_Advanced_Configuration_Optimizer.git
cd SAO_Advanced_Configuration_Optimizer
python run.py
```

That's it. The launcher creates a virtual environment, installs dependencies, and opens `http://localhost:5555` in your browser. From there the app walks you through entering your dbt Cloud credentials.

> Use `--port 8080` to change the port: `python run.py --port 8080`

## What you'll need from dbt Cloud

| Field | Where to find it |
|---|---|
| Account Prefix | Your dbt Cloud URL subdomain — `abc123` from `abc123.us1.dbt.com` |
| Account ID | In the URL: `/deploy/{account_id}/...` |
| Project ID | In the URL: `/projects/{project_id}/...` |
| Environment ID | Your **production** deployment environment ID |
| Service Token | Account Settings > Service Tokens > create with **read** + **metadata** access |

All credentials are stored locally in `config/credentials.json` (gitignored). Nothing leaves your machine except API calls to your own dbt Cloud instance.

## What it does

| Tab | Purpose |
|---|---|
| **Dashboard** | Per-model execution stats, costs, row changes, and job schedules across all production jobs |
| **Models Optimization** | Surfaces table models that could benefit from incremental materialization |
| **Jobs Optimization** | Detects overlapping jobs that could be consolidated |
| **Updated At** | Helps choose which upstream tables to use for `updates_on` configuration |
| **Build After** | Identifies models building too frequently — candidates for longer `build_after` intervals |
| **Data Dictionary** | Reference definitions for every field in the dashboard |
| **Warehouse Costs** | Map your warehouse sizes to cost tiers for spend estimation |

## Stopping the app

`Ctrl+C` in the terminal.
