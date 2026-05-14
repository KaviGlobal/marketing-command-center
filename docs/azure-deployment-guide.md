# Azure Deployment Guide

Last reviewed: 2026-05-13

This document covers the following deployment areas:

- Python Azure Functions and scheduling
- Power BI dashboard
- Overall Azure configuration

Do not paste secrets into this document. Record names, IDs, URLs, schedules, and owners here, but keep keys, passwords, and tokens in approved secret storage.

## 1. Python Azure Functions & Scheduling

### 1.1 What is implemented in this repository

- The Azure Function App is an HTTP intake layer implemented in [function_app.py](../function_app.py).
- It accepts chatbot/session writes and stores canonical JSON in Azure Blob Storage.
- The ingestion worker is a separate Python process implemented in [blob_text_to_azure_sql.py](../blob_text_to_azure_sql.py).
- Scheduling for blob-to-SQL ingestion is therefore handled using Container Apps Job.

Current backend flow:

1. Client or orchestration layer calls the Azure Function HTTP endpoint.
2. The Function App writes JSON into Blob Storage.
3. A scheduled worker ingests blobs into Azure SQL.
4. The worker refreshes `dbo.kpi_aggregates` by calling `dbo.usp_refresh_kpi_aggregates`.

### 1.2 Function App runtime details

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

### 1.3 Function endpoints

The Function App exposes these routes:

- `GET /api/health`
- `POST /api/session-log/upsert-field`
- `POST /api/session-log/upsert-batch`
- `GET /api/session-log/get-session?sessionId=<GUID>`

Authentication:

- `GET /api/health` is anonymous
- the other routes require a Function key

### 1.4 Azure portal steps for the Function App

In the Azure portal:

1. Search for `Function App`.
2. Click the production Function App:
   - **`kavi-chatbot-etl`**
3. In the left navigation, open `Overview`.
4. Confirm:
   - subscription
   - resource group
   - region
   - runtime stack
   - running status
5. In the left navigation, open `Configuration`.
6. Under `Application settings`, confirm the settings listed in section `1.5`.
7. In the left navigation, open `Functions`.
8. Confirm the HTTP functions are present.
9. In the left navigation, open `App keys`.
10. Confirm there is a function key strategy documented for the protected routes.
11. In the left navigation, open `Monitoring > Log stream`.
12. Trigger `GET /api/health` and confirm the app responds successfully.

### 1.5 Function App settings to configure

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

Production details:

- **Production Function App name:** `kavi-chatbot-etl`
- **Production Function App URL / default domain:** `https://kavi-chatbot-etl-bqfxgpd9acaqe9b6.centralus-01.azurewebsites.net`
- **How to find App keys:** `Function App → kavi-chatbot-etl → Functions → App keys`


- Status: `Running`
- Operating system: `Linux`
- Plan type: `Flex Consumption`
- Instance memory: `512 MB`

### 1.6 Deployment steps for the Function App
- **`[FILL IN: deployment method used for the Function App]`**


Portal validation steps after deploy:

1. Open the Function App in Azure.
2. Go to `Overview`.
3. Confirm `Status` is `Running`.
4. Go to `Functions`.
5. Confirm the expected HTTP functions appear.
6. Go to `Log stream`.
7. Call:
   - `GET /api/health`
8. Confirm HTTP `200` and healthy output.

Confirmed from the current Azure portal screenshots:

- `get_session` - enabled
- `health` - enabled
- `upsert_batch` - enabled
- `upsert_field` - enabled

### 1.7 Scheduling the ingestion worker

Important implementation note:
- The scheduled process is the ingestion worker in [blob_text_to_azure_sql.py](../blob_text_to_azure_sql.py).
- The scheduler currently in use for blob-to-SQL ingestion is an Azure Container Apps Job.

Primary worker command:

```bash
python blob_text_to_azure_sql.py
```

Current production schedule:

- **Scheduler implementation:** `Azure Container Apps Job`
- **Approved production schedule in local time:** `Daily at 2:00 AM America/Chicago`
- **Approved production cron in UTC:** `0 7 * * *`

### 1.8 Azure portal steps for the scheduler

In the Azure portal:

1. Search for `Container Apps Jobs`.
2. Open the job:
   - **`kavi-blob-ingestion-job`**
3. In `Overview`, confirm:
   - resource group
   - region
   - provisioning state
4. Open `Configuration`.
5. Confirm:
   - trigger type is `Schedule`
   - cron expression matches the approved schedule
   - image points to the correct ACR image
6. Open `Identity`.
7. Confirm the system-assigned managed identity is enabled.
8. Open `Execution history`.
9. Confirm the latest run completed successfully.
10. Open the linked Container Apps environment if needed.
11. Confirm environment region and logging configuration.

### 1.9 Scheduler environment variables

The ingestion worker requires:

- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`
- `AZURE_SQL_CONN_STR` or:
  - `AZURE_SQL_SERVER`
  - `AZURE_SQL_DATABASE`
  - `AZURE_SQL_USER`
  - `AZURE_SQL_PASSWORD`

Recommended values:

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

Production details to record:

- **Container Apps environment name:** `copilot-chatbot-jobs-env`
- **Container Registry name:** `copilotchatbot91b7acr`
- **Scheduler Image Name and Tag:** `blob-ingestion:20260511-1`
- **Where to find Scheduler Secrets:**
`Container Apps Job → kavi-blob-ingestion-job → Settings/Security → Secrets`
- **Job Monitoring/Alert Owner:`[TBD]`**

Implementation notes:

- The ingestion worker image can now be built directly from this repo using `Dockerfile`.
- `.dockerignore` excludes local secrets and transient files from the image build context.
- Raw ingestion accepts non-GUID `sessionId` values and deterministically maps them to GUIDs for normalized reporting tables.
- Canonical `flow_type` is treated as a session-level value.
- Generic wrapper hints such as `System` and `ConversationEvaluation` are ignored for canonical business flow resolution.

### 1.10 Post-deployment validation for Functions and scheduler

Run these checks after deployment:

1. `GET /api/health` returns `200`.
2. A test write successfully stores a session blob.
3. The scheduled ingestion job runs successfully.
4. `dbo.session_blob_ingestion_run` shows a completed run.
5. `dbo.kpi_aggregate_refresh_run` shows a completed refresh.
6. Blob ingestion logs contain no unresolved SQL or storage errors.
7. Confirm `blobs_processed > 0` and that the scheduler storage account/prefix matches the actual blob source.

## 2. Power BI Dashboard

### 2.1 What the dashboard should read

From the documented ingestion and reporting flow in this repository, the intended read path is:

1. `dbo.vw_kpi_card_base_power_bi` for top KPI cards and Month-over-Month card labels
2. `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi` for trends, flow-level reporting, and other precomputed aggregate visuals
3. `dbo.vw_session_heatmap_power_bi` for the weekday/hour session heatmap
4. `dbo.vw_session_reporting_detail` for drill-down detail
5. `dbo.prospect_inquiries` for top-offering detail

The dashboard should **not** read directly from raw blob payloads.

For report-build conventions such as KPI card MoM behavior and heatmap source design, also see [powerbi.md](powerbi.md).

### 2.2 Expected Power BI data sources

Primary dataset objects:

- `dbo.vw_kpi_card_base_power_bi`
- `dbo.kpi_aggregates`
- `dbo.vw_kpi_aggregates_power_bi`
- `dbo.vw_session_heatmap_power_bi`
- `dbo.vw_session_reporting_detail`
- `dbo.prospect_inquiries`


### 2.3 Power BI deployment checklist

Confirm these items:

1. The report is connected to the production Azure SQL database.
2. The dataset uses the correct production server and database names.
3. Credentials are configured in the Power BI Service.
4. Scheduled refresh is configured.
5. Visuals load correctly after the latest ingestion run.
6. Any drill-through or detail page uses the intended SQL-backed data.

### 2.4 Power BI Service click path

In the Power BI Service at `app.powerbi.com`:

1. Open the workspace:
   - **`api-capstone`**
2. Open the dataset or semantic model:
   - **`TO BE PUBLISHED`**
3. Click `Settings`.
4. In `Data source credentials`, confirm the production SQL credentials or gateway mapping is correct.
5. In `Gateway and cloud connections`, confirm the production SQL source is mapped correctly.
6. In `Scheduled refresh`, confirm:
   - refresh is enabled
   - time zone is correct
   - refresh times align with the ingestion schedule
7. Go back to the workspace and open the report:
   - **`chatbot_dashboard`**
8. Confirm the dashboard/report visuals load without source errors.

### 2.5 Recommended refresh sequencing

Because the ingestion worker refreshes SQL aggregates after a successful run, Power BI should refresh **after** the ingestion schedule completes.

Recommended sequence:

1. Azure Container Apps Job ingests blobs
2. `dbo.usp_refresh_kpi_aggregates` completes
3. Power BI scheduled refresh runs


### 2.6 Visual-to-source mapping

Recommended mapping based on this repository:

- KPI cards: `dbo.vw_kpi_card_base_power_bi`
- KPI card MoM labels: `dbo.vw_kpi_card_base_power_bi`
- trend charts: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- flow split visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- outcome visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- inquiry type visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- session heatmap: `dbo.vw_session_heatmap_power_bi`
- top offerings visuals: `dbo.prospect_inquiries`
- session drill-down table: `dbo.vw_session_reporting_detail`

### 2.7 Power BI validation queries

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


## 3. Overall Azure Configuration

### 3.1 Core Azure resources in scope

This solution depends on:

- Azure Function App
- Azure Storage Account
- Azure SQL Server and Database
- Azure Container Registry
- Azure Container Apps environment
- Azure Container Apps Job

Production resource details to document:

- **Subscription name:** `Marketing Command Center`
- **Subscription ID:** `bd0b1d2f-8e14-49bf-bbde-d4073d05f1f5`
- **Resource group name:** `copilot-chatbot`
- **Azure region:** `Central US`

### 3.2 Azure portal navigation checklist

Use the Azure portal to verify each resource:

1. Search for `Resource groups`.
2. Open the production resource group:
   - **`[FILL IN: resource group name]`**
3. In the resource group `Overview`, confirm all expected resources exist.
4. Open the Function App and complete the checks in section `1`.
5. Open the Storage Account and complete the checks in section `3.3`.
6. Open the SQL Database and complete the checks in section `3.4`.
7. Open the Container Registry and complete the checks in section `3.5`.
8. Open the Container Apps Job and complete the checks in section `1.8`.

### 3.3 Storage Account configuration

Expected usage:

- raw session blobs are stored in Blob Storage
- default container name is `session-logs`
- blob names may live at container root or under `sessions/`
- optional index prefix is `session-path-index/`

In the Azure portal:

1. Search for `Storage accounts`.
2. Open the production storage account:
   - `copilotchatbot91b7`
3. Open `Data storage > Containers`.
4. Confirm the `session-logs` container exists.
5. Open the container and confirm the expected folder structure exists:
   - root-level blob names, or
   - `sessions/`
   - optionally `session-path-index/`
6. Match the scheduler app settings to the actual blob layout:
   - use blank `SESSION_LOG_BLOB_PREFIX` for root-level blob names
   - use `SESSION_LOG_BLOB_PREFIX=sessions/` only when blob names start with `sessions/`
7. Open `Security + networking`.
8. Confirm public access, firewall, and network rules match production policy.
9. Open `Access keys` or the team’s approved secret source only if you need to rotate credentials. Do not paste keys into this document.

### 3.4 Azure SQL configuration

Required SQL objects are created by [chatbot-sessions-data-export-schema.sql](../chatbot-sessions-data-export-schema.sql).

In the Azure portal:

1. Search for `SQL databases`.
2. Open the production database:
   - **`[FILL IN: SQL database name]`**
3. On `Overview`, confirm:
   - database name
   - server name
   - pricing tier
   - status
4. Click the linked SQL server.
5. Open `Networking`.
6. Confirm the ingestion runtime and any required Azure services can reach SQL.
7. Return to the SQL database and open `Query editor` or use SSMS/Azure Data Studio.
8. Verify that these objects exist:
   - `dbo.kpi_aggregates`
   - `dbo.vw_kpi_aggregates_power_bi`
   - `dbo.vw_kpi_card_base_power_bi`
   - `dbo.vw_session_heatmap_power_bi`
   - `dbo.vw_session_reporting_detail`
   - `dbo.kpi_aggregate_refresh_run`
   - `dbo.session_blob_ingestion_run`
   - `dbo.usp_refresh_kpi_aggregates`

Record:

- **SQL server name:** `kavi-chatbot-sessions.database.windows.net`
- **SQL database name:** `copilot-chat-sessions-db`
- **`[FILL IN: authentication method used in production]`**
- **`[FILL IN: firewall / networking owner]`**

### 3.5 Azure Container Registry configuration

In the Azure portal:

1. Search for `Container registries`.
2. Open the production registry:
   - `copilotchatbot91b7acr`
3. Open `Repositories`.
4. Confirm the ingestion image repository exists.
5. Confirm the expected tag exists.
6. Open `Access control (IAM)`.
7. Confirm the Container Apps Job managed identity has `AcrPull`.

Record:

- **ACR name:** `copilotchatbot91b7acr`
- **`[FILL IN: image repository name]`**
- **`[FILL IN: production image tag]`**

### 3.6 Configuration values that must exist

These settings should be present somewhere in approved configuration management:

- Function App storage connection
- scheduler storage connection
- scheduler SQL connection
- Function App key management
- schedule definition
- Power BI production source mapping


### 3.7 Final Azure validation

Before calling the deployment complete, confirm:

1. The Function App is healthy.
2. The storage container receives session blobs.
3. The scheduled ingestion job executes successfully.
4. SQL ingestion tracking tables update after a run.
5. KPI aggregate refresh tracking updates after a run.
6. Power BI refresh runs after ingestion and the report loads correctly.
