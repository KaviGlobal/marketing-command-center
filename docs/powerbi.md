# Power BI and Reporting

## Purpose

This page documents the SQL reporting layer that Power BI should read from for chatbot analytics.

Power BI should read from reporting tables and helper views in Azure SQL. It should not read directly from raw blob payloads or from `dbo.session_blob_fact` unless you are doing ad hoc debugging.

## Reporting objects

### Core reporting tables and views

- `dbo.kpi_aggregates`
  - Pre-aggregated KPI table refreshed by `dbo.usp_refresh_kpi_aggregates`.
  - Best for trend lines, flow splits, outcome charts, and other precomputed aggregate visuals.

- `dbo.vw_kpi_aggregates_power_bi`
  - Power BI-friendly projection of `dbo.kpi_aggregates`.
  - Preserves compatibility aliases such as `engaged_session_rate`.

- `dbo.vw_session_reporting_detail`
  - Session-level drill-down view.
  - Best for detail tables and drill-through pages.

- `dbo.prospect_inquiries`
  - Best for top-offering and prospect-specific detail.

### Dedicated helper views for current Power BI visuals

- `dbo.vw_kpi_card_base_power_bi`
  - Session-level base view for top KPI cards and Month-over-Month labels.
  - Supports counts, rates, and CSAT card logic while letting the main KPI respect the full slicer selection.

- `dbo.vw_session_heatmap_power_bi`
  - Dedicated heatmap view for sessions by day of week and hour.
  - Derives weekday labels, weekday sort order, hour labels, hour sort order, flow type, and distinct session counts from `dbo.sessions`.

## Recommended visual-to-source mapping

- KPI cards: `dbo.vw_kpi_card_base_power_bi`
- KPI card MoM labels: `dbo.vw_kpi_card_base_power_bi`
- Trend charts: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- Flow split visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- Outcome visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- Inquiry type visuals: `dbo.kpi_aggregates` or `dbo.vw_kpi_aggregates_power_bi`
- Top offerings visuals: `dbo.prospect_inquiries`
- Session heatmap: `dbo.vw_session_heatmap_power_bi`
- Session drill-down table: `dbo.vw_session_reporting_detail`

## KPI card behavior

### Main KPI value

The large KPI card value should use the full slicer selection.

Example:

```text
Month slicer = 3-5
Main KPI = combined result across March, April, and May
```

### Month-over-Month label

MoM should compare the latest selected month to the immediately previous month.

Example:

```text
Selected Month range: 3-5
Current month for MoM = Month 5
Baseline month = Month 4
```

This is MoM. Comparing the selected period to the prior multi-month period is period-over-period, not MoM.

### Recommended MoM display rules

- Count cards such as `Total Sessions`: show relative percent change.
- Rate cards such as replied, completed flow, and escalation: show percentage-point change.
- Score cards such as CSAT: show decimal score change.

### Slicer note

If the report also uses a Week slicer, decide explicitly whether top KPI cards should respect it.

- If Week affects the KPI cards, MoM may become harder to explain.
- If Week is intended mainly for detailed visuals, keep MoM cards month-driven and limit Week slicer impact there.

## Heatmap behavior

The heatmap should be built from `dbo.vw_session_heatmap_power_bi`, not from `dbo.kpi_aggregates`.

Recommended visual setup:

- Rows: day of week
- Columns: hour labels in 12-hour format
- Values: session count
- Filters:
  - `flow_type` in `Career`, `Partnership`, `Prospect`
  - `hour_sort` between `7` and `17` when you want a business-hours view

Use a title that makes the timezone explicit, for example:

```text
Sessions by Day of Week and Hour (UTC)
```

## Refresh sequencing

Recommended order:

1. Session blobs are ingested into SQL.
2. `dbo.usp_refresh_kpi_aggregates` completes.
3. Power BI semantic model refresh runs.

The helper views do not need their own refresh job because they read directly from SQL objects that are already updated by ingestion and aggregate refresh.

## Validation queries

### Aggregate/reporting layer

```sql
SELECT TOP 10 *
FROM dbo.kpi_aggregates
ORDER BY metric_period_start DESC;

SELECT TOP 10 *
FROM dbo.vw_session_reporting_detail
ORDER BY metric_date DESC;
```

### KPI cards base view

```sql
SELECT TOP 20 *
FROM dbo.vw_kpi_card_base_power_bi
ORDER BY metric_date DESC;
```

### Heatmap view

```sql
SELECT TOP 20 *
FROM dbo.vw_session_heatmap_power_bi
ORDER BY day_of_week_sort, hour_sort;
```

### Heatmap cross-check

```sql
SELECT
    day_of_week_name,
    hour_12_label,
    SUM(total_sessions) AS sessions
FROM dbo.vw_session_heatmap_power_bi
WHERE hour_sort BETWEEN 7 AND 17
  AND flow_type IN ('Career', 'Partnership', 'Prospect')
GROUP BY
    day_of_week_name,
    day_of_week_sort,
    hour_12_label,
    hour_sort
ORDER BY
    day_of_week_sort,
    hour_sort;
```

## Maintenance note

Keep Power BI helper views aligned with the canonical SQL reporting layer. When reporting requirements change, update:

- `chatbot-sessions-data-export-schema.sql`
- this `powerbi.md` guide
- any deployment or Azure validation docs that reference report source objects
