-- =============================================================
-- Kavi Chat Data Export — Azure SQL Schema (Production)
-- This is the canonical DDL. All tables below are already
-- deployed to the production Azure SQL database.
-- =============================================================

-- Historical prospect/offering migration deltas are already
-- included in this file, so it is the only Azure SQL script
-- that should be used going forward.

CREATE TABLE dbo.session_blob_session (
    session_id NVARCHAR(200) PRIMARY KEY,
    bot_id NVARCHAR(200) NULL,
    user_id NVARCHAR(200) NULL,
    user_email NVARCHAR(320) NULL,
    user_display_name NVARCHAR(500) NULL,
    created_at_utc DATETIME2(7) NULL,
    last_updated_utc DATETIME2(7) NULL,
    inserted_at_utc DATETIME2(7) NOT NULL
        CONSTRAINT DF_session_blob_session_inserted_at DEFAULT (SYSUTCDATETIME())
);

CREATE INDEX IX_session_blob_session_user_email
    ON dbo.session_blob_session (user_email);


CREATE TABLE dbo.session_blob_fact (
    fact_id INT IDENTITY(1,1) PRIMARY KEY,
    blob_path NVARCHAR(512) NOT NULL,
    session_id NVARCHAR(200) NOT NULL,
    field_name NVARCHAR(150) NOT NULL,
    field_value NVARCHAR(MAX) NULL,
    is_kpi BIT NOT NULL,
    event_type NVARCHAR(50) NOT NULL,
    event_timestamp_utc DATETIME2(7) NULL,
    inserted_at_utc DATETIME2(7) NOT NULL
        CONSTRAINT DF_session_blob_fact_inserted_at DEFAULT (SYSUTCDATETIME())
);

CREATE INDEX IX_session_blob_fact_blob_path
    ON dbo.session_blob_fact (blob_path);

CREATE INDEX IX_session_blob_fact_session_id
    ON dbo.session_blob_fact (session_id);


CREATE TABLE dbo.session_blob_rejection (
    rejection_id INT IDENTITY(1,1) PRIMARY KEY,
    blob_path NVARCHAR(512) NOT NULL,
    session_id NVARCHAR(200) NULL,
    field_name NVARCHAR(150) NULL,
    rejected_at_utc DATETIME2(7) NOT NULL
        CONSTRAINT DF_session_blob_rejection_rejected_at DEFAULT (SYSUTCDATETIME()),
    reason NVARCHAR(1000) NOT NULL,
    raw_text NVARCHAR(MAX) NULL
);

CREATE INDEX IX_session_blob_rejection_blob_path
    ON dbo.session_blob_rejection (blob_path);


CREATE TABLE dbo.session_blob_ingestion (
    blob_path NVARCHAR(512) PRIMARY KEY,
    last_modified_utc DATETIME2(7) NOT NULL,
    blob_etag NVARCHAR(100) NOT NULL,
    ingestion_status NVARCHAR(32) NOT NULL,
    ingested_at_utc DATETIME2(7) NOT NULL
        CONSTRAINT DF_session_blob_ingestion_ingested_at DEFAULT (SYSUTCDATETIME()),
    row_count INT NOT NULL,
    rejection_count INT NOT NULL,
    error_message NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.session_blob_ingestion_run (
    run_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    started_at_utc DATETIME2(7) NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_started DEFAULT (SYSUTCDATETIME()),
    completed_at_utc DATETIME2(7) NULL,
    run_status NVARCHAR(32) NOT NULL,
    selected_blob_path NVARCHAR(512) NULL,
    blobs_processed INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_processed DEFAULT (0),
    blobs_succeeded INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_succeeded DEFAULT (0),
    blobs_rejected INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_rejected DEFAULT (0),
    blobs_failed INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_failed DEFAULT (0),
    blobs_skipped INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_skipped DEFAULT (0),
    sql_connect_retries INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_connect_retries DEFAULT (0),
    sql_execute_retries INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_execute_retries DEFAULT (0),
    sql_executemany_retries INT NOT NULL
        CONSTRAINT DF_session_blob_ingestion_run_executemany_retries DEFAULT (0),
    error_message NVARCHAR(MAX) NULL
);

CREATE TABLE dbo.kpi_aggregate_refresh_run (
    run_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    started_at_utc DATETIME2(7) NOT NULL
        CONSTRAINT DF_kpi_aggregate_refresh_run_started DEFAULT (SYSUTCDATETIME()),
    completed_at_utc DATETIME2(7) NULL,
    refresh_status NVARCHAR(32) NOT NULL,
    lookback_days INT NOT NULL,
    full_refresh BIT NOT NULL,
    window_start_utc DATETIME2(3) NULL,
    window_end_utc DATETIME2(3) NULL,
    rows_inserted INT NULL,
    procedure_name NVARCHAR(256) NOT NULL,
    error_message NVARCHAR(MAX) NULL
);


CREATE TABLE dbo.sessions (
    session_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_start_time_utc DATETIME2(3) NULL,
    session_end_time_utc DATETIME2(3) NULL,

    engaged_flag BIT NULL,
    resolved_flag BIT NULL,
    escalated_flag BIT NULL,
    abandoned_flag BIT NULL,

    flow_type VARCHAR(100) NULL,
    satisfaction_score INT NULL,
    response_latency_ms_avg INT NULL,
    error_flag BIT NULL,
    fallback_flag BIT NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_sessions_created_at DEFAULT (SYSUTCDATETIME())
);


CREATE TABLE dbo.drop_off_nodes (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    last_node_id VARCHAR(100) NULL,
    last_node_name VARCHAR(255) NULL,
    last_node_time_utc DATETIME2(3) NULL,

    goal_completed_flag BIT NULL,
    exit_reason VARCHAR(255) NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_drop_off_nodes_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_drop_off_nodes_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.satisfaction_feedback (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    satisfaction_score INT NULL,
    feedback_submitted_flag BIT NULL,
    satisfaction_submitted_flag BIT NULL,
    feedback_comment NVARCHAR(MAX) NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_satisfaction_feedback_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_satisfaction_feedback_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.prospect_inquiries (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    flow_type VARCHAR(100) NULL,

    lead_capture_started_flag BIT NULL,
    lead_capture_completed_flag BIT NULL,

    lead_name VARCHAR(200) NULL,
    lead_email VARCHAR(320) NULL,
    lead_company VARCHAR(255) NULL,
    lead_phone VARCHAR(50) NULL,
    lead_industry VARCHAR(200) NULL,
    lead_job_title VARCHAR(200) NULL,

    consultation_requested_flag BIT NULL,
    scheduler_link_clicked_flag BIT NULL,

    offering_primary VARCHAR(200) NULL,
    offering_secondary VARCHAR(200) NULL,
    offering_primary_category VARCHAR(100) NULL,
    offering_secondary_category VARCHAR(100) NULL,

    intent_primary VARCHAR(200) NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_prospect_inquiries_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_prospect_inquiries_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.career_inquiries (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    flow_type VARCHAR(100) NULL,

    application_intent_flag BIT NULL,

    candidate_capture_started_flag BIT NULL,
    candidate_capture_completed_flag BIT NULL,

    candidate_name VARCHAR(200) NULL,
    candidate_email VARCHAR(320) NULL,

    job_interest_area VARCHAR(200) NULL,
    job_interest_location VARCHAR(200) NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_career_inquiries_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_career_inquiries_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.partner_inquiries (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    flow_type VARCHAR(100) NULL,

    partner_capture_started_flag BIT NULL,
    partner_capture_completed_flag BIT NULL,

    partner_name VARCHAR(200) NULL,
    partner_org_name VARCHAR(255) NULL,
    partner_email VARCHAR(320) NULL,

    partner_type VARCHAR(200) NULL,

    partner_consultation_requested_flag BIT NULL,
    partner_consultation_booked_flag BIT NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_partner_inquiries_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_partner_inquiries_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.vendor_inquiries (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    vendor_name VARCHAR(200) NULL,
    vendor_company VARCHAR(255) NULL,
    vendor_email VARCHAR(320) NULL,
    vendor_phone VARCHAR(50) NULL,
    service_category VARCHAR(200) NULL,
    service_details NVARCHAR(MAX) NULL,
    partner_status VARCHAR(50) NULL, -- new/existing
    previous_experience NVARCHAR(MAX) NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_vendor_inquiries_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_vendor_inquiries_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.bot_optimization_metrics (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    session_id UNIQUEIDENTIFIER NOT NULL,

    fallback_flag BIT NULL,
    fallback_count INT NULL,

    response_latency_ms INT NULL,

    error_flag BIT NULL,
    error_node_id INT NULL,
    error_code VARCHAR(100) NULL,
    error_count INT NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_bot_optimization_metrics_created_at DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT FK_bot_optimization_metrics_sessions
        FOREIGN KEY (session_id) REFERENCES dbo.sessions(session_id)
);


CREATE TABLE dbo.kpi_aggregates (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,

    metric_period_start DATETIME2(3) NULL,
    metric_period_end DATETIME2(3) NULL,

    year INT NULL,
    month INT NULL,
    week_of_year INT NULL,
    flow_type VARCHAR(100) NULL,
    time_bucket_hour INT NULL,
    day_of_week INT NULL,

    session_outcome_category VARCHAR(64) NULL,
    total_sessions INT NULL,
    replied_at_least_once_rate FLOAT NULL,
    engaged_session_rate FLOAT NULL, -- deprecated compatibility alias for replied_at_least_once_rate
    completed_flow_rate FLOAT NULL,
    escalation_rate FLOAT NULL,
    abandonment_rate FLOAT NULL,
    avg_satisfaction_score FLOAT NULL,

    prospect_inquiry_count INT NULL,
    career_inquiry_count INT NULL,
    partner_inquiry_count INT NULL,

    prospect_info_capture_rate FLOAT NULL,
    leads_conversion_rate FLOAT NULL,

    applicant_inquiry_count INT NULL,
    application_intent_count INT NULL,

    partner_info_capture_rate FLOAT NULL,

    fallback_rate FLOAT NULL,
    avg_response_latency_ms FLOAT NULL,
    error_rate FLOAT NULL,

    engaged_sessions_count INT NULL,
    completed_sessions_count INT NULL,
    converted_sessions_count INT NULL,
    converted_sessions_rate FLOAT NULL,
    unique_users_count INT NULL,
    total_session_time_seconds BIGINT NULL,
    time_spent_per_unique_user_seconds FLOAT NULL,

    created_at DATETIME2(3) NOT NULL
        CONSTRAINT DF_kpi_aggregates_created_at DEFAULT (SYSUTCDATETIME())
);

GO

CREATE OR ALTER VIEW dbo.vw_session_reporting_detail
AS
    SELECT
        s.session_id,
        CAST(s.created_at AS DATE) AS metric_date,
        COALESCE(s.flow_type, 'unknown') AS flow_type,
        CASE
            WHEN dn.goal_completed_flag = 1 THEN 'completed_flow'
            WHEN s.fallback_flag = 1 THEN 'system_fallback'
            WHEN s.abandoned_flag = 1 THEN 'left_midway'
            ELSE 'unknown'
        END AS session_outcome_category,
        s.session_start_time_utc,
        s.session_end_time_utc,
        lbs.blob_path AS blob_path,
        CONCAT('sessionId=', CONVERT(VARCHAR(36), s.session_id)) AS drilldown_reference
    FROM dbo.sessions s
    LEFT JOIN dbo.drop_off_nodes dn
        ON dn.session_id = s.session_id
    OUTER APPLY (
        SELECT TOP (1)
            f.blob_path
        FROM dbo.session_blob_fact f
        WHERE f.session_id = CONVERT(NVARCHAR(200), s.session_id)
        ORDER BY f.inserted_at_utc DESC, f.fact_id DESC
    ) lbs;
GO

CREATE OR ALTER VIEW dbo.vw_kpi_aggregates_power_bi
AS
    SELECT
        id,
        metric_period_start,
        metric_period_end,
        year,
        month,
        week_of_year,
        flow_type,
        day_of_week,
        session_outcome_category,
        total_sessions,
        replied_at_least_once_rate,
        replied_at_least_once_rate AS engaged_session_rate, -- compatibility for existing BI reports
        completed_flow_rate,
        escalation_rate,
        abandonment_rate,
        avg_satisfaction_score,
        prospect_inquiry_count,
        career_inquiry_count,
        partner_inquiry_count,
        prospect_info_capture_rate,
        leads_conversion_rate,
        applicant_inquiry_count,
        application_intent_count,
        partner_info_capture_rate,
        fallback_rate,
        avg_response_latency_ms,
        error_rate,
        engaged_sessions_count,
        completed_sessions_count,
        converted_sessions_count,
        converted_sessions_rate,
        unique_users_count,
        total_session_time_seconds,
        time_spent_per_unique_user_seconds,
        created_at
    FROM dbo.kpi_aggregates;

GO

CREATE OR ALTER PROCEDURE dbo.usp_refresh_kpi_aggregates
    @LookbackDays INT = 30,
    @FullRefresh BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    -- Stabilize week/weekday calculations across environments.
    -- Monday=1, Sunday=7 for DATEPART(WEEKDAY, ...).
    SET DATEFIRST 1;

    DECLARE @RunId UNIQUEIDENTIFIER = NEWID();
    DECLARE @WindowStart DATETIME2(3);
    DECLARE @WindowEnd   DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @RowsInserted INT = 0;

    IF @FullRefresh = 1
        SET @WindowStart = '1900-01-01';
    ELSE
        SET @WindowStart = DATEADD(DAY, -@LookbackDays, CAST(@WindowEnd AS DATE));

    INSERT INTO dbo.kpi_aggregate_refresh_run (
        run_id,
        started_at_utc,
        refresh_status,
        lookback_days,
        full_refresh,
        window_start_utc,
        window_end_utc,
        procedure_name
    )
    VALUES (
        @RunId,
        SYSUTCDATETIME(),
        'Started',
        @LookbackDays,
        @FullRefresh,
        @WindowStart,
        @WindowEnd,
        'dbo.usp_refresh_kpi_aggregates'
    );

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Clear the window we're about to rebuild
        DELETE FROM dbo.kpi_aggregates
        WHERE metric_period_start >= @WindowStart
           OR (@FullRefresh = 1);

        ;WITH session_snapshot AS (
            SELECT
                s.session_id,
                CAST(s.created_at AS DATE)                            AS metric_date,
                COALESCE(s.flow_type, 'unknown')                      AS flow_type,
                CASE
                    WHEN dn.goal_completed_flag = 1 THEN 'completed_flow'
                    WHEN s.fallback_flag = 1 THEN 'system_fallback'
                    WHEN s.abandoned_flag = 1 THEN 'left_midway'
                    ELSE 'unknown'
                END                                                   AS session_outcome_category,
                s.engaged_flag,
                s.resolved_flag,
                s.escalated_flag,
                s.abandoned_flag,
                s.satisfaction_score,
                s.response_latency_ms_avg,
                s.error_flag,
                s.fallback_flag,
                CASE
                    WHEN s.session_start_time_utc IS NOT NULL
                      AND s.session_end_time_utc IS NOT NULL
                      AND s.session_end_time_utc >= s.session_start_time_utc
                    THEN DATEDIFF(SECOND, s.session_start_time_utc, s.session_end_time_utc)
                    ELSE NULL
                END                                                   AS session_duration_seconds,
                COALESCE(
                    NULLIF(sb.user_id, ''),
                    NULLIF(sb.user_email, ''),
                    CONVERT(NVARCHAR(200), s.session_id)
                )                                                     AS unique_user_key,
                dn.goal_completed_flag,
                pi.lead_capture_started_flag,
                pi.lead_capture_completed_flag,
                pi.consultation_requested_flag,
                ci.application_intent_flag,
                ci.candidate_capture_started_flag,
                ci.candidate_capture_completed_flag,
                pa.partner_capture_started_flag,
                pa.partner_capture_completed_flag,
                CASE WHEN pi.session_id IS NOT NULL THEN 1 ELSE 0 END AS is_prospect,
                CASE WHEN ci.session_id IS NOT NULL THEN 1 ELSE 0 END AS is_career,
                CASE WHEN pa.session_id IS NOT NULL THEN 1 ELSE 0 END AS is_partner,
                CASE WHEN vi.session_id IS NOT NULL THEN 1 ELSE 0 END AS is_vendor,
                CASE
                    WHEN pi.lead_capture_completed_flag = 1
                      OR pi.consultation_requested_flag = 1
                      OR ci.candidate_capture_completed_flag = 1
                      OR pa.partner_capture_completed_flag = 1
                    THEN 1 ELSE 0
                END AS converted_flag
            FROM dbo.sessions s
            LEFT JOIN dbo.drop_off_nodes dn       ON dn.session_id = s.session_id
            LEFT JOIN dbo.session_blob_session sb ON sb.session_id = CONVERT(NVARCHAR(200), s.session_id)
            LEFT JOIN dbo.prospect_inquiries pi   ON pi.session_id = s.session_id
            LEFT JOIN dbo.career_inquiries   ci   ON ci.session_id = s.session_id
            LEFT JOIN dbo.partner_inquiries  pa   ON pa.session_id = s.session_id
            LEFT JOIN dbo.vendor_inquiries   vi   ON vi.session_id = s.session_id
            WHERE s.created_at >= @WindowStart
        )
        INSERT INTO dbo.kpi_aggregates (
            id,
            metric_period_start,
            metric_period_end,
            year,
            month,
            week_of_year,
            flow_type,
            session_outcome_category,
            day_of_week,
            total_sessions,
            replied_at_least_once_rate,
            engaged_session_rate,
            completed_flow_rate,
            escalation_rate,
            abandonment_rate,
            avg_satisfaction_score,
            prospect_inquiry_count,
            career_inquiry_count,
            partner_inquiry_count,
            prospect_info_capture_rate,
            leads_conversion_rate,
            applicant_inquiry_count,
            application_intent_count,
            partner_info_capture_rate,
            fallback_rate,
            avg_response_latency_ms,
            error_rate,
            engaged_sessions_count,
            completed_sessions_count,
            converted_sessions_count,
            converted_sessions_rate,
            unique_users_count,
            total_session_time_seconds,
            time_spent_per_unique_user_seconds,
            created_at
        )
        SELECT
            NEWID(),
            CAST(metric_date AS DATETIME2(3))                         AS metric_period_start,
            DATEADD(DAY, 1, CAST(metric_date AS DATETIME2(3)))        AS metric_period_end,
            DATEPART(YEAR, metric_date)                               AS year,
            DATEPART(MONTH, metric_date)                              AS month,
            DATEPART(ISO_WEEK, metric_date)                           AS week_of_year,
            flow_type,
            session_outcome_category,
            DATEPART(WEEKDAY, metric_date)                            AS day_of_week,
            COUNT(*)                                                  AS total_sessions,
            CAST(SUM(CASE WHEN engaged_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS replied_at_least_once_rate,
            CAST(SUM(CASE WHEN engaged_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS engaged_session_rate,
            CAST(SUM(CASE WHEN goal_completed_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS completed_flow_rate,
            CAST(SUM(CASE WHEN escalated_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS escalation_rate,
            CAST(SUM(CASE WHEN abandoned_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS abandonment_rate,
            AVG(CAST(satisfaction_score AS FLOAT))                    AS avg_satisfaction_score,
            SUM(is_prospect)                                          AS prospect_inquiry_count,
            SUM(is_career)                                            AS career_inquiry_count,
            SUM(is_partner)                                           AS partner_inquiry_count,
            CAST(SUM(CASE WHEN lead_capture_completed_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(SUM(CASE WHEN lead_capture_started_flag = 1 THEN 1 ELSE 0 END), 0)
                                                                      AS prospect_info_capture_rate,
            CAST(SUM(CASE WHEN lead_capture_completed_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS leads_conversion_rate,
            SUM(is_career)                                            AS applicant_inquiry_count,
            SUM(CASE WHEN application_intent_flag = 1 THEN 1 ELSE 0 END) AS application_intent_count,
            CAST(SUM(CASE WHEN partner_capture_completed_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(SUM(CASE WHEN partner_capture_started_flag = 1 THEN 1 ELSE 0 END), 0)
                                                                      AS partner_info_capture_rate,
            CAST(SUM(CASE WHEN fallback_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS fallback_rate,
            AVG(CAST(response_latency_ms_avg AS FLOAT))               AS avg_response_latency_ms,
            CAST(SUM(CASE WHEN error_flag = 1 THEN 1 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS error_rate,
            SUM(CASE WHEN engaged_flag = 1 THEN 1 ELSE 0 END)         AS engaged_sessions_count,
            SUM(CASE WHEN goal_completed_flag = 1 THEN 1 ELSE 0 END)  AS completed_sessions_count,
            SUM(converted_flag)                                       AS converted_sessions_count,
            CAST(SUM(converted_flag) AS FLOAT)
                / NULLIF(COUNT(*), 0)                                 AS converted_sessions_rate,
            COUNT(DISTINCT unique_user_key)                           AS unique_users_count,
            SUM(COALESCE(session_duration_seconds, 0))                AS total_session_time_seconds,
            CAST(SUM(COALESCE(session_duration_seconds, 0)) AS FLOAT)
                / NULLIF(COUNT(DISTINCT unique_user_key), 0)          AS time_spent_per_unique_user_seconds,
            SYSUTCDATETIME()                                          AS created_at
        FROM session_snapshot
        GROUP BY metric_date, flow_type, session_outcome_category;

        SET @RowsInserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        UPDATE dbo.kpi_aggregate_refresh_run
        SET
            completed_at_utc = SYSUTCDATETIME(),
            refresh_status = 'Succeeded',
            rows_inserted = @RowsInserted
        WHERE run_id = @RunId;

        SELECT @RowsInserted AS rows_inserted, @RunId AS refresh_run_id;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        UPDATE dbo.kpi_aggregate_refresh_run
        SET
            completed_at_utc = SYSUTCDATETIME(),
            refresh_status = 'Failed',
            error_message = CONCAT('Error ', ERROR_NUMBER(), ': ', ERROR_MESSAGE())
        WHERE run_id = @RunId;

        THROW;
    END CATCH
END
GO
