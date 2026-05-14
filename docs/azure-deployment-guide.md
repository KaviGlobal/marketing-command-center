# Azure Deployment Guide

Last reviewed: 2026-05-13

This document is the Azure-side runbook for the chatbot ingestion system and its Power BI reporting layer.

Use this guide to verify:

- the Azure Function App that accepts session writes
- the Azure Container Apps Job that ingests blobs into SQL
- the Azure SQL objects that back reporting
- the Power BI report and semantic model wiring

Do not paste secrets into this document. Record names, URLs, IDs, schedules, and owners here, but keep credentials and keys in approved secret storage.

## 1. System Summary

### 1.1 What runs where

The production chatbot pipeline in this repo is split into two runtimes:

1. The Azure Function App receives session writes over HTTP.
2. The Function App writes canonical session JSON into Azure Blob Storage.
3. A scheduled Azure Container Apps Job runs the ingestion worker.
4. The ingestion worker validates blobs, writes normalized data into Azure SQL, and refreshes aggregate KPI data.

Current backend flow:

```text
Client / orchestration layer
  -> Azure Function App
  -> Azure Blob Storage
  -> Azure Container Apps Job
  -> Azure SQL Database
  -> Power BI
```

### 1.2 Production resource snapshot

Known from the repo and existing deployment notes:

| Area | Production value |
|---|---|
| Subscription name | `Marketing Command Center` |
| Subscription ID | `bd0b1d2f-8e14-49bf-bbde-d4073d05f1f5` |
| Resource group | `copilot-chatbot` |
| Region | `Central US` |
| Function App name | `kavi-chatbot-etl` |
| Function App URL | `https://kavi-chatbot-etl-bqfxgpd9acaqe9b6.centralus-01.azurewebsites.net` |
| Storage account | `copilotchatbot91b7` |
| Blob container | `session-logs` |
| Container Apps environment | `copilot-chatbot-jobs-env` |
| Container Apps Job | `kavi-blob-ingestion-job` |
| Container Registry | `copilotchatbot91b7acr` |
| Scheduler image | `blob-ingestion:20260511-1` |
| SQL server | `kavi-chatbot-sessions.database.windows.net` |
| SQL database | `copilot-chat-sessions-db` |
| Power BI workspace | `api-capstone` |
| Power BI report | `chatbot_dashboard` |

## 2. Function App

### 2.1 Implementation in this repo

- The Function App is implemented in [function_app.py](../function_app.py).
- It accepts chatbot/session writes and stores canonical JSON in Azure Blob Storage.
- It does not write directly to Azure SQL.
- It writes merged session documents, typically under `sessions/<sessionId>.json` unless storage layout or partitioning settings change.

### 2.2 Runtime details

From [host.json](../host.json):

- Route prefix: `api`
- Timeout: `00:05:00`
- `maxOutstandingRequests`: `200`
- `maxConcurrentRequests`: `100`

From [requirements.txt](../requirements.txt):

- `azure-functions>=1.21.0`
- `azure-storage-blob>=12.19.0`
- `azure-core>=1.30.0`
- `pyodbc>=5.1.0`

Runtime expectation:

- Python `3.13.x`
- OS: `Linux`
- Plan type: `Flex Consumption`
- Instance memory: `512 MB`
- Status: `Running`

### 2.3 Endpoints and auth

Expected HTTP endpoints:

- `GET /api/health`
- `POST /api/session-log/upsert-field`
- `POST /api/session-log/upsert-batch`
- `GET /api/session-log/get-session?sessionId=<GUID>`

Authentication:

- `GET /api/health` is anonymous
- the other routes require a Function key

### 2.4 Function App settings

Required:

- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`

Recommended / expected:

- `SESSION_LOG_CONTAINER=session-logs`
- `SESSION_LOG_DATE_PARTITION=false`
- `SESSION_LOG_MAX_RETRIES=3`
- `SESSION_LOG_PARTITION_LOOKUP_FALLBACK=true`
- `SESSION_LOG_PARTITION_LOOKUP_RECENT_DAYS=7`
- `SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS=2000`
- `SESSION_LOG_PARTITION_LOOKUP_SCAN_PAGE_SIZE=500`
- `SESSION_LOG_PARTITION_LOOKUP_CACHE_TTL_SECONDS=300`
- `SESSION_LOG_PARTITION_LOOKUP_CACHE_MAX_ENTRIES=5000`
- `SESSION_LOG_PATH_INDEX_ENABLED=true`
- `SESSION_LOG_PATH_INDEX_PREFIX=session-path-index/`

### 2.5 Portal verification steps

In the Azure portal:

1. Search for `Function App`.
2. Open `kavi-chatbot-etl`.
3. In `Overview`, confirm:
   - subscription
   - resource group
   - region
   - runtime stack
   - running status
4. In `Configuration`, confirm the settings in section `2.4`.
5. In `Functions`, confirm these functions exist:
   - `get_session`
   - `health`
   - `upsert_batch`
   - `upsert_field`
6. In `App keys`, confirm there is a documented key strategy for protected routes.
7. In `Monitoring > Log stream`, trigger `GET /api/health` and confirm a successful response.

### 2.6 Function App deployment note

The current deployment guidance for the Function App is:

- Recommended: deploy from VS Code using the Azure Functions extension
- CLI alternative: publish with Azure Functions Core Tools

Recommended deployment flow:

1. Open the project folder in VS Code.
2. Press `Ctrl+Shift+P`.
3. Run `Azure Functions: Deploy to Function App`.
4. Select the Azure subscription.
5. Select `kavi-chatbot-etl`.
6. Confirm deployment when prompted.
7. Wait for `Deployment successful`.

CLI alternative:

```bash
func azure functionapp publish kavi-chatbot-etl
```

## 3. Azure Container Apps Job

### 3.1 Scheduler role

The scheduled ingestion worker is implemented in [blob_text_to_azure_sql.py](../blob_text_to_azure_sql.py).

Important implementation notes:

- The scheduler currently in use for blob-to-SQL ingestion is an Azure Container Apps Job.
- The primary worker command is:

```bash
python blob_text_to_azure_sql.py
```

- The worker refreshes `dbo.kpi_aggregates` by calling `dbo.usp_refresh_kpi_aggregates`.
- The worker can read the main merged-session blob layout and legacy one-blob-per-field layouts when compatibility mode is enabled.

For a developer-focused local setup and one-off local execution flow, use the ingestion worker section in [DEPLOYMENT.md](../DEPLOYMENT.md). This Azure guide stays focused on the deployed Container Apps Job and related cloud checks.

### 3.2 Scheduler settings and schedule

Current production scheduler configuration:

- Scheduler implementation: `Azure Container Apps Job`
- Job name: `kavi-blob-ingestion-job`
- Container Apps environment: `copilot-chatbot-jobs-env`
- Container Registry: `copilotchatbot91b7acr`
- Image: `blob-ingestion:20260511-1`
- Approved production schedule in local time: `Daily at 2:00 AM America/Chicago`
- Approved production cron in UTC: `0 7 * * *`

### 3.3 Scheduler environment variables

Required:

- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`
- `AZURE_SQL_CONN_STR` or:
  - `AZURE_SQL_SERVER`
  - `AZURE_SQL_DATABASE`
  - `AZURE_SQL_USER`
  - `AZURE_SQL_PASSWORD`

Recommended:

- `SESSION_LOG_CONTAINER=session-logs`
- `SESSION_LOG_BLOB_PREFIX=` when blobs live at the root of `session-logs`
- `SESSION_LOG_BLOB_PREFIX=sessions/` only when blob names actually start with `sessions/`
- `KPI_AGGREGATE_REFRESH_ENABLED=true`
- `KPI_AGGREGATE_REFRESH_LOOKBACK_DAYS=30`
- `KPI_AGGREGATE_REFRESH_FULL=false`
- `KPI_AGGREGATE_REFRESH_FAIL_ON_ERROR=false`
- `FACT_ROW_DEDUPE_ENABLED=true`
- `INGESTION_LIST_PAGE_SIZE=500`
- `INGESTION_DELETE_AFTER_SUCCESS=false`
- `DEAD_LETTER_DELETE_SOURCE=false`
- `INGESTION_LOG_JSON_ENABLED=true`

Also relevant from the current worker behavior:

- `SESSION_LOG_COMPAT_LEGACY_ENABLED=true` if legacy blob sources must still be scanned
- `SESSION_LOG_LEGACY_CONTAINER=session-logs` if legacy blobs live in the same container
- `SESSION_LOG_LEGACY_PREFIX=` when legacy blobs live at container root

### 3.4 Portal verification steps

In the Azure portal:

1. Search for `Container Apps Jobs`.
2. Open `kavi-blob-ingestion-job`.
3. In `Overview`, confirm:
   - resource group
   - region
   - provisioning state
4. In `Configuration`, confirm:
   - trigger type is `Schedule`
   - cron expression matches `0 7 * * *`
   - image points to the correct ACR image
5. In `Identity`, confirm the system-assigned managed identity is enabled.
6. In `Execution history`, confirm the latest run completed successfully.
7. If needed, open the linked Container Apps environment and confirm regional/logging alignment.
8. In `Settings/Security > Secrets`, confirm the job has the required storage and SQL secret references.

### 3.5 Cloud Shell commands

Use these from Azure Cloud Shell to inspect the job without clicking through the portal.

Set common variables first:

```bash
RG="copilot-chatbot"
JOB="kavi-blob-ingestion-job"
ENV_NAME="copilot-chatbot-jobs-env"
ACR_NAME="copilotchatbot91b7acr"
IMAGE_REPO="blob-ingestion"
```

Show the job summary:

```bash
az containerapp job show \
  --resource-group "$RG" \
  --name "$JOB" \
  --output table
```

Check trigger type, cron, and image:

```bash
az containerapp job show \
  --resource-group "$RG" \
  --name "$JOB" \
  --query "{triggerType:properties.configuration.triggerType,cron:properties.configuration.scheduleTriggerConfig.cronExpression,image:properties.template.containers[0].image}" \
  --output yaml
```

List recent executions:

```bash
az containerapp job execution list \
  --resource-group "$RG" \
  --name "$JOB" \
  --output table
```

Start the job manually:

```bash
az containerapp job start \
  --resource-group "$RG" \
  --name "$JOB"
```

Inspect the managed identity configuration:

```bash
az containerapp job show \
  --resource-group "$RG" \
  --name "$JOB" \
  --query identity \
  --output yaml
```

Inspect the Container Apps environment:

```bash
az containerapp env show \
  --resource-group "$RG" \
  --name "$ENV_NAME" \
  --output table
```

Check available ACR tags for the ingestion image:

```bash
az acr repository show-tags \
  --name "$ACR_NAME" \
  --repository "$IMAGE_REPO" \
  --output table
```

### 3.6 Additional production details to record

Known:

- Where to find scheduler secrets:
  `Container Apps Job -> kavi-blob-ingestion-job -> Settings/Security -> Secrets`

## 4. Azure Storage

### 4.1 Expected usage

- Raw session blobs are stored in Blob Storage.
- Default container name is `session-logs`.
- Blob names may live at container root or under `sessions/`.
- Optional path index prefix is `session-path-index/`.
- The current Function App writes merged session documents, while the ingestion worker can also scan legacy root-level or alternate-prefix blob layouts for compatibility.

### 4.2 Portal verification steps

In the Azure portal:

1. Search for `Storage accounts`.
2. Open `copilotchatbot91b7`.
3. Open `Data storage > Containers`.
4. Confirm the `session-logs` container exists.
5. Inspect the container and confirm the actual blob layout in production:
   - root-level blob names, or
   - `sessions/`, and
   - optionally `session-path-index/`
6. Match the scheduler app settings to the actual blob layout:
   - use blank `SESSION_LOG_BLOB_PREFIX` for root-level blob names
   - use `SESSION_LOG_BLOB_PREFIX=sessions/` only when blob names start with `sessions/`
7. In `Security + networking`, confirm public access, firewall, and network rules match production policy.
8. Use `Access keys` or the team’s approved secret source only if rotation is required. Do not paste keys into this document.

## 5. Azure SQL

### 5.1 Schema source of truth

Required SQL objects are created by [chatbot-sessions-data-export-schema.sql](../chatbot-sessions-data-export-schema.sql).

This includes:

- raw ingestion tables
- normalized reporting tables
- reporting helper views
- KPI aggregate refresh procedure and run-history tables

### 5.2 Portal verification steps

In the Azure portal:

1. Search for `SQL databases`.
2. Open `copilot-chat-sessions-db`.
3. In `Overview`, confirm:
   - database name
   - server name
   - pricing tier
   - status
4. Open the linked SQL server.
5. In `Networking`, confirm the Function App runtime, Container Apps Job runtime, and any approved Azure services can reach SQL.
6. Return to the database and open `Query editor`, SSMS, or Azure Data Studio.
7. Verify these objects exist:
   - `dbo.kpi_aggregates`
   - `dbo.vw_kpi_aggregates_power_bi`
   - `dbo.vw_kpi_card_base_power_bi`
   - `dbo.vw_session_heatmap_power_bi`
   - `dbo.vw_session_reporting_detail`
   - `dbo.kpi_aggregate_refresh_run`
   - `dbo.session_blob_ingestion_run`
   - `dbo.usp_refresh_kpi_aggregates`

### 5.3 Known production values

- SQL server name: `kavi-chatbot-sessions.database.windows.net`
- SQL database name: `copilot-chat-sessions-db`

## 6. Azure Container Registry

### 6.1 Portal verification steps

In the Azure portal:

1. Search for `Container registries`.
2. Open `copilotchatbot91b7acr`.
3. Open `Repositories`.
4. Confirm the ingestion image repository exists.
5. Confirm the expected production tag exists.
6. Open `Access control (IAM)`.
7. Confirm the Container Apps Job managed identity has `AcrPull`.

### 6.2 Known production values

- ACR name: `copilotchatbot91b7acr`
- inferred image repository name: `blob-ingestion`
- production image tag: `20260511-1`

## 7. Power BI

### 7.1 Intended SQL source objects

From the reporting model in this repo, the intended read path is:

1. `dbo.vw_kpi_card_base_power_bi` for top KPI cards and Month-over-Month card labels
2. `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi` for trends, flow-level reporting, and other precomputed aggregate visuals
3. `dbo.vw_session_heatmap_power_bi` for the weekday/hour session heatmap
4. `dbo.vw_session_reporting_detail` for drill-down detail
5. `dbo.prospect_inquiries` for top-offering detail

The dashboard should not read directly from raw blob payloads.

For report-build conventions such as KPI card MoM behavior and heatmap source design, also see [powerbi.md](powerbi.md).

### 7.2 Expected dataset objects

- `dbo.vw_kpi_card_base_power_bi`
- `dbo.kpi_aggregates`
- `dbo.vw_kpi_aggregates_power_bi`
- `dbo.vw_session_heatmap_power_bi`
- `dbo.vw_session_reporting_detail`
- `dbo.prospect_inquiries`

### 7.3 Known production Power BI values

- Workspace: `api-capstone`
- Semantic model / dataset: `Youtube x GA4 x Facebook v2`
- Report: `chatbot_dashboard`

### 7.4 Service verification steps

In Power BI Service at `app.powerbi.com`:

1. Open the `api-capstone` workspace.
2. Open `Youtube x GA4 x Facebook v2`.
3. In `Settings`, confirm:
   - data source credentials are correct
   - gateway or cloud connections are mapped correctly
   - scheduled refresh is enabled
   - refresh timing aligns with the ingestion schedule
4. Open `chatbot_dashboard`.
5. Confirm visuals load without source errors.

### 7.5 Recommended refresh sequencing

Recommended order:

1. Azure Container Apps Job ingests blobs.
2. `dbo.usp_refresh_kpi_aggregates` completes.
3. Power BI scheduled refresh runs.

### 7.6 Visual-to-source mapping

- KPI cards: `dbo.vw_kpi_card_base_power_bi`
- KPI card MoM labels: `dbo.vw_kpi_card_base_power_bi`
- trend charts: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- flow split visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- outcome visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- inquiry type visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- session heatmap: `dbo.vw_session_heatmap_power_bi`
- top offerings visuals: `dbo.prospect_inquiries`
- session drill-down table: `dbo.vw_session_reporting_detail`

### 7.7 Validation queries

Use these against production SQL if the report appears stale:

```sql
SELECT TOP 10 *
FROM dbo.kpi_aggregates
ORDER BY metric_period_start DESC;

SELECT TOP 10 *
FROM dbo.vw_kpi_card_base_power_bi
ORDER BY metric_date DESC;

SELECT TOP 10 *
FROM dbo.vw_session_heatmap_power_bi
ORDER BY metric_date DESC, day_of_week_sort, hour_sort;

SELECT TOP 10 *
FROM dbo.vw_session_reporting_detail
ORDER BY metric_date DESC;
```

## 8. Final Validation Checklist

Before calling the deployment healthy, confirm:

1. The Function App is healthy and `GET /api/health` returns `200`.
2. A test write stores a session blob successfully.
3. The storage container shows the expected blob layout.
4. The scheduled ingestion job executes successfully.
5. `dbo.session_blob_ingestion_run` shows a completed run.
6. `dbo.kpi_aggregate_refresh_run` shows a completed refresh.
7. Blob ingestion logs contain no unresolved SQL or storage errors.
8. Power BI refresh runs after ingestion and the report loads correctly.
