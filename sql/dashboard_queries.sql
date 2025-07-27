-- Enhanced Cost Tracking Dashboard Queries
-- =======================================
-- This file contains optimized SQL queries for the advanced cost tracking dashboard
-- with support for cost trends, breakdowns, anomalies, forecasts, and multi-region data.

-- 1. COST TRENDS AND FORECASTS
-- ============================

-- Cost trends with 7-day moving average and forecasts
CREATE OR REPLACE VIEW cost_tracking.dashboard_cost_trends AS
WITH cost_data AS (
    SELECT 
        project_id,
        usage_date,
        region,
        total_cost,
        dbu_cost,
        infra_cost,
        compute_type,
        LAG(total_cost, 1) OVER (PARTITION BY project_id ORDER BY usage_date) AS prev_day_cost,
        LAG(total_cost, 7) OVER (PARTITION BY project_id ORDER BY usage_date) AS week_ago_cost,
        AVG(total_cost) OVER (PARTITION BY project_id ORDER BY usage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg,
        AVG(total_cost) OVER (PARTITION BY project_id ORDER BY usage_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_avg
    FROM cost_tracking.project_costs
    WHERE usage_date >= current_date - interval '30 days'
),
forecast_data AS (
    SELECT 
        project_id,
        forecast_date AS usage_date,
        predicted_cost AS total_cost,
        confidence_lower,
        confidence_upper,
        'forecast' AS data_type
    FROM cost_tracking.forecasts
    WHERE forecast_date >= current_date
      AND forecast_date <= current_date + interval '7 days'
)
SELECT 
    project_id,
    usage_date,
    region,
    total_cost,
    dbu_cost,
    infra_cost,
    compute_type,
    prev_day_cost,
    week_ago_cost,
    rolling_7day_avg,
    rolling_30day_avg,
    CASE 
        WHEN prev_day_cost > 0 THEN ((total_cost - prev_day_cost) / prev_day_cost) * 100
        ELSE NULL 
    END AS day_over_day_change_pct,
    CASE 
        WHEN week_ago_cost > 0 THEN ((total_cost - week_ago_cost) / week_ago_cost) * 100
        ELSE NULL 
    END AS week_over_week_change_pct,
    'actual' AS data_type,
    NULL AS confidence_lower,
    NULL AS confidence_upper
FROM cost_data
UNION ALL
SELECT 
    project_id,
    usage_date,
    NULL AS region,
    total_cost,
    NULL AS dbu_cost,
    NULL AS infra_cost,
    NULL AS compute_type,
    NULL AS prev_day_cost,
    NULL AS week_ago_cost,
    NULL AS rolling_7day_avg,
    NULL AS rolling_30day_avg,
    NULL AS day_over_day_change_pct,
    NULL AS week_over_week_change_pct,
    data_type,
    confidence_lower,
    confidence_upper
FROM forecast_data
ORDER BY project_id, usage_date;

-- 2. COST BREAKDOWN BY COMPUTE TYPE
-- =================================

-- Cost breakdown by compute type (pie chart)
CREATE OR REPLACE VIEW cost_tracking.dashboard_compute_breakdown AS
SELECT 
    compute_type,
    region,
    SUM(total_cost) AS total_cost,
    SUM(dbu_cost) AS total_dbu_cost,
    SUM(infra_cost) AS total_infra_cost,
    COUNT(DISTINCT project_id) AS project_count,
    COUNT(DISTINCT cluster_id) AS cluster_count,
    AVG(total_cost) AS avg_daily_cost
FROM cost_tracking.project_costs
WHERE usage_date >= current_date - interval '7 days'
GROUP BY compute_type, region
ORDER BY total_cost DESC;

-- 3. ANOMALY DETECTION RESULTS
-- ============================

-- Recent anomalies with details
CREATE OR REPLACE VIEW cost_tracking.dashboard_anomalies AS
SELECT 
    a.anomaly_id,
    a.project_id,
    a.usage_date,
    a.actual_cost,
    a.expected_cost,
    a.anomaly_score,
    a.anomaly_type,
    a.severity,
    a.description,
    a.detected_at,
    a.status,
    p.region,
    p.compute_type,
    CASE 
        WHEN a.expected_cost > 0 THEN ((a.actual_cost - a.expected_cost) / a.expected_cost) * 100
        ELSE NULL 
    END AS cost_deviation_pct,
    CASE 
        WHEN a.severity = 'critical' THEN 4
        WHEN a.severity = 'high' THEN 3
        WHEN a.severity = 'medium' THEN 2
        WHEN a.severity = 'low' THEN 1
        ELSE 0
    END AS severity_score
FROM cost_tracking.anomalies a
LEFT JOIN cost_tracking.project_costs p 
    ON a.project_id = p.project_id 
    AND a.usage_date = p.usage_date
WHERE a.usage_date >= current_date - interval '30 days'
ORDER BY a.detected_at DESC, severity_score DESC;

-- 4. PROJECT SUMMARY DASHBOARD
-- =============================

-- Project cost summary with trends
CREATE OR REPLACE VIEW cost_tracking.dashboard_project_summary AS
WITH project_stats AS (
    SELECT 
        project_id,
        region,
        COUNT(DISTINCT usage_date) AS days_with_data,
        SUM(total_cost) AS total_cost_7d,
        SUM(dbu_cost) AS total_dbu_cost_7d,
        SUM(infra_cost) AS total_infra_cost_7d,
        AVG(total_cost) AS avg_daily_cost,
        MAX(total_cost) AS max_daily_cost,
        MIN(total_cost) AS min_daily_cost,
        STDDEV(total_cost) AS cost_stddev,
        COUNT(DISTINCT cluster_id) AS active_clusters,
        MAX(usage_date) AS last_usage_date
    FROM cost_tracking.project_costs
    WHERE usage_date >= current_date - interval '7 days'
    GROUP BY project_id, region
),
project_anomalies AS (
    SELECT 
        project_id,
        COUNT(*) AS anomaly_count,
        COUNT(CASE WHEN severity IN ('high', 'critical') THEN 1 END) AS high_severity_anomalies,
        MAX(detected_at) AS last_anomaly_date
    FROM cost_tracking.anomalies
    WHERE usage_date >= current_date - interval '7 days'
      AND status = 'open'
    GROUP BY project_id
),
project_forecasts AS (
    SELECT 
        project_id,
        AVG(predicted_cost) AS avg_forecasted_cost,
        SUM(predicted_cost) AS total_forecasted_cost,
        MIN(predicted_cost) AS min_forecasted_cost,
        MAX(predicted_cost) AS max_forecasted_cost
    FROM cost_tracking.forecasts
    WHERE forecast_date >= current_date
      AND forecast_date <= current_date + interval '7 days'
    GROUP BY project_id
)
SELECT 
    ps.project_id,
    ps.region,
    ps.days_with_data,
    ps.total_cost_7d,
    ps.total_dbu_cost_7d,
    ps.total_infra_cost_7d,
    ps.avg_daily_cost,
    ps.max_daily_cost,
    ps.min_daily_cost,
    ps.cost_stddev,
    ps.active_clusters,
    ps.last_usage_date,
    COALESCE(pa.anomaly_count, 0) AS anomaly_count,
    COALESCE(pa.high_severity_anomalies, 0) AS high_severity_anomalies,
    pa.last_anomaly_date,
    pf.avg_forecasted_cost,
    pf.total_forecasted_cost,
    pf.min_forecasted_cost,
    pf.max_forecasted_cost,
    CASE 
        WHEN pf.avg_forecasted_cost > 0 AND ps.avg_daily_cost > 0 
        THEN ((pf.avg_forecasted_cost - ps.avg_daily_cost) / ps.avg_daily_cost) * 100
        ELSE NULL 
    END AS forecasted_change_pct,
    CASE 
        WHEN ps.cost_stddev > 0 AND ps.avg_daily_cost > 0 
        THEN (ps.cost_stddev / ps.avg_daily_cost) * 100
        ELSE 0 
    END AS cost_volatility_pct
FROM project_stats ps
LEFT JOIN project_anomalies pa ON ps.project_id = pa.project_id
LEFT JOIN project_forecasts pf ON ps.project_id = pf.project_id
ORDER BY ps.total_cost_7d DESC;

-- 5. REGIONAL COST ANALYSIS
-- =========================

-- Regional cost comparison
CREATE OR REPLACE VIEW cost_tracking.dashboard_regional_analysis AS
SELECT 
    region,
    COUNT(DISTINCT project_id) AS project_count,
    SUM(total_cost) AS total_cost_7d,
    SUM(dbu_cost) AS total_dbu_cost_7d,
    SUM(infra_cost) AS total_infra_cost_7d,
    AVG(total_cost) AS avg_daily_cost_per_project,
    COUNT(DISTINCT cluster_id) AS total_clusters,
    COUNT(DISTINCT compute_type) AS compute_types_used,
    MAX(usage_date) AS last_activity_date
FROM cost_tracking.project_costs
WHERE usage_date >= current_date - interval '7 days'
GROUP BY region
ORDER BY total_cost_7d DESC;

-- 6. COST OPTIMIZATION RECOMMENDATIONS
-- ====================================

-- Active optimization recommendations
CREATE OR REPLACE VIEW cost_tracking.dashboard_optimization_recommendations AS
SELECT 
    recommendation_id,
    project_id,
    recommendation_type,
    title,
    description,
    potential_savings,
    priority,
    status,
    created_at,
    CASE 
        WHEN priority = 'high' THEN 3
        WHEN priority = 'medium' THEN 2
        WHEN priority = 'low' THEN 1
        ELSE 0
    END AS priority_score
FROM cost_tracking.optimization_recommendations
WHERE status = 'pending'
  AND created_at >= current_date - interval '7 days'
ORDER BY priority_score DESC, potential_savings DESC;

-- 7. PIPELINE HEALTH MONITORING
-- =============================

-- Pipeline execution health
CREATE OR REPLACE VIEW cost_tracking.dashboard_pipeline_health AS
WITH recent_executions AS (
    SELECT 
        pipeline_name,
        status,
        execution_time_seconds,
        rows_processed,
        start_time,
        end_time,
        error_message,
        CASE 
            WHEN status = 'success' THEN 1
            WHEN status = 'partial' THEN 0.5
            ELSE 0
        END AS success_score
    FROM cost_tracking.pipeline_logs
    WHERE start_time >= current_date - interval '7 days'
),
pipeline_stats AS (
    SELECT 
        pipeline_name,
        COUNT(*) AS total_executions,
        SUM(success_score) AS successful_executions,
        AVG(execution_time_seconds) AS avg_execution_time,
        MAX(execution_time_seconds) AS max_execution_time,
        SUM(rows_processed) AS total_rows_processed,
        MAX(start_time) AS last_execution,
        COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) AS error_count
    FROM recent_executions
    GROUP BY pipeline_name
)
SELECT 
    pipeline_name,
    total_executions,
    successful_executions,
    CASE 
        WHEN total_executions > 0 THEN (successful_executions / total_executions) * 100
        ELSE 0
    END AS success_rate_pct,
    avg_execution_time,
    max_execution_time,
    total_rows_processed,
    last_execution,
    error_count,
    CASE 
        WHEN success_rate_pct >= 95 THEN 'excellent'
        WHEN success_rate_pct >= 90 THEN 'good'
        WHEN success_rate_pct >= 80 THEN 'fair'
        ELSE 'poor'
    END AS health_status
FROM pipeline_stats
ORDER BY success_rate_pct DESC;

-- 8. AUDIT AND ACCESS LOGS
-- ========================

-- Recent dashboard access logs
CREATE OR REPLACE VIEW cost_tracking.dashboard_access_logs AS
SELECT 
    audit_id,
    user_id,
    action,
    resource_type,
    resource_id,
    ip_address,
    user_agent,
    accessed_at,
    DATE(accessed_at) AS access_date,
    HOUR(accessed_at) AS access_hour
FROM cost_tracking.audit_logs
WHERE accessed_at >= current_date - interval '7 days'
ORDER BY accessed_at DESC;

-- 9. PERFORMANCE OPTIMIZED QUERIES
-- ================================

-- Materialized view for frequently accessed cost data
CREATE MATERIALIZED VIEW IF NOT EXISTS cost_tracking.cost_summary_cache
AS
SELECT 
    project_id,
    region,
    compute_type,
    DATE_TRUNC('week', usage_date) AS week_start,
    SUM(total_cost) AS weekly_total_cost,
    SUM(dbu_cost) AS weekly_dbu_cost,
    SUM(infra_cost) AS weekly_infra_cost,
    AVG(total_cost) AS avg_daily_cost,
    COUNT(DISTINCT usage_date) AS days_with_data,
    COUNT(DISTINCT cluster_id) AS unique_clusters
FROM cost_tracking.project_costs
WHERE usage_date >= current_date - interval '90 days'
GROUP BY project_id, region, compute_type, DATE_TRUNC('week', usage_date);

-- 10. DASHBOARD FILTERS AND PARAMETERS
-- ====================================

-- Available projects for filtering
CREATE OR REPLACE VIEW cost_tracking.dashboard_projects AS
SELECT DISTINCT
    project_id,
    region,
    MAX(usage_date) AS last_activity,
    COUNT(DISTINCT usage_date) AS active_days
FROM cost_tracking.project_costs
WHERE usage_date >= current_date - interval '30 days'
GROUP BY project_id, region
ORDER BY last_activity DESC;

-- Available regions for filtering
CREATE OR REPLACE VIEW cost_tracking.dashboard_regions AS
SELECT DISTINCT
    region,
    COUNT(DISTINCT project_id) AS project_count,
    MAX(usage_date) AS last_activity
FROM cost_tracking.project_costs
WHERE usage_date >= current_date - interval '30 days'
GROUP BY region
ORDER BY project_count DESC;

-- 11. SAMPLE DASHBOARD QUERIES FOR SPECIFIC CHARTS
-- ================================================

-- Line chart: Cost trends over time
-- Usage: SELECT * FROM cost_tracking.dashboard_cost_trends WHERE project_id = 'ProjectA' ORDER BY usage_date;

-- Pie chart: Cost breakdown by compute type
-- Usage: SELECT * FROM cost_tracking.dashboard_compute_breakdown;

-- Bar chart: Project cost comparison
-- Usage: SELECT project_id, total_cost_7d FROM cost_tracking.dashboard_project_summary ORDER BY total_cost_7d DESC;

-- Table: Recent anomalies
-- Usage: SELECT * FROM cost_tracking.dashboard_anomalies WHERE severity IN ('high', 'critical') ORDER BY detected_at DESC;

-- Gauge: Pipeline health
-- Usage: SELECT pipeline_name, success_rate_pct FROM cost_tracking.dashboard_pipeline_health;

-- 12. EXPORT QUERIES FOR POWER BI
-- ===============================

-- Power BI export: Project costs with all dimensions
CREATE OR REPLACE VIEW cost_tracking.powerbi_project_costs AS
SELECT 
    pc.*,
    a.anomaly_count,
    a.high_severity_anomalies,
    f.avg_forecasted_cost,
    f.total_forecasted_cost,
    orr.potential_savings,
    orr.recommendation_count
FROM cost_tracking.project_costs pc
LEFT JOIN (
    SELECT 
        project_id,
        COUNT(*) AS anomaly_count,
        COUNT(CASE WHEN severity IN ('high', 'critical') THEN 1 END) AS high_severity_anomalies
    FROM cost_tracking.anomalies
    WHERE usage_date >= current_date - interval '7 days'
    GROUP BY project_id
) a ON pc.project_id = a.project_id
LEFT JOIN (
    SELECT 
        project_id,
        AVG(predicted_cost) AS avg_forecasted_cost,
        SUM(predicted_cost) AS total_forecasted_cost
    FROM cost_tracking.forecasts
    WHERE forecast_date >= current_date
    GROUP BY project_id
) f ON pc.project_id = f.project_id
LEFT JOIN (
    SELECT 
        project_id,
        SUM(potential_savings) AS potential_savings,
        COUNT(*) AS recommendation_count
    FROM cost_tracking.optimization_recommendations
    WHERE status = 'pending'
    GROUP BY project_id
) orr ON pc.project_id = orr.project_id
WHERE pc.usage_date >= current_date - interval '30 days'; 