-- Enhanced Cost Tracking System - Delta Table Schemas
-- This file creates all necessary tables for the advanced cost tracking dashboard

-- Create schema
CREATE SCHEMA IF NOT EXISTS cost_tracking;

-- Main project costs table (enhanced)
CREATE TABLE IF NOT EXISTS cost_tracking.project_costs (
  project_id STRING,
  usage_date DATE,
  workspace_id STRING,
  region STRING,
  dbu_cost DECIMAL(10,2),
  infra_cost DECIMAL(10,2),
  total_cost DECIMAL(10,2),
  compute_type STRING, -- All-Purpose, Jobs, SQL Warehouse
  cluster_id STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (usage_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Anomaly detection results
CREATE TABLE IF NOT EXISTS cost_tracking.anomalies (
  anomaly_id STRING,
  project_id STRING,
  usage_date DATE,
  actual_cost DECIMAL(10,2),
  expected_cost DECIMAL(10,2),
  anomaly_score DECIMAL(5,4),
  anomaly_type STRING, -- 'spike', 'drop', 'trend_change'
  severity STRING, -- 'low', 'medium', 'high', 'critical'
  description STRING,
  detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  status STRING DEFAULT 'open' -- 'open', 'acknowledged', 'resolved'
)
USING DELTA
PARTITIONED BY (usage_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Cost forecasting results
CREATE TABLE IF NOT EXISTS cost_tracking.forecasts (
  forecast_id STRING,
  project_id STRING,
  forecast_date DATE,
  predicted_cost DECIMAL(10,2),
  confidence_lower DECIMAL(10,2),
  confidence_upper DECIMAL(10,2),
  model_version STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (forecast_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Pipeline execution logs
CREATE TABLE IF NOT EXISTS cost_tracking.pipeline_logs (
  log_id STRING,
  pipeline_name STRING,
  execution_id STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING, -- 'success', 'failed', 'partial'
  rows_processed BIGINT,
  error_message STRING,
  execution_time_seconds DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (date(start_time))
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Workspace configuration for multi-region support
CREATE TABLE IF NOT EXISTS cost_tracking.workspace_config (
  workspace_id STRING,
  workspace_name STRING,
  region STRING,
  dbu_rate_all_purpose DECIMAL(5,2),
  dbu_rate_jobs DECIMAL(5,2),
  dbu_rate_sql_warehouse DECIMAL(5,2),
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- User permissions for row-level security
CREATE TABLE IF NOT EXISTS cost_tracking.user_permissions (
  user_id STRING,
  project_id STRING,
  permission_level STRING, -- 'read', 'write', 'admin'
  granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  granted_by STRING,
  is_active BOOLEAN DEFAULT true
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Audit logs for dashboard access
CREATE TABLE IF NOT EXISTS cost_tracking.audit_logs (
  audit_id STRING,
  user_id STRING,
  action STRING, -- 'dashboard_view', 'data_export', 'anomaly_acknowledge'
  resource_type STRING, -- 'dashboard', 'table', 'anomaly'
  resource_id STRING,
  ip_address STRING,
  user_agent STRING,
  accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (date(accessed_at))
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Cost optimization recommendations
CREATE TABLE IF NOT EXISTS cost_tracking.optimization_recommendations (
  recommendation_id STRING,
  project_id STRING,
  recommendation_type STRING, -- 'cluster_termination', 'instance_downgrade', 'storage_cleanup'
  title STRING,
  description STRING,
  potential_savings DECIMAL(10,2),
  priority STRING, -- 'low', 'medium', 'high'
  status STRING DEFAULT 'pending', -- 'pending', 'implemented', 'dismissed'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  implemented_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (date(created_at))
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Create indexes for better query performance
-- Note: These are materialized views in Databricks SQL
CREATE MATERIALIZED VIEW IF NOT EXISTS cost_tracking.project_costs_daily_summary
AS
SELECT
  project_id,
  usage_date,
  region,
  SUM(dbu_cost) AS total_dbu_cost,
  SUM(infra_cost) AS total_infra_cost,
  SUM(total_cost) AS total_cost,
  COUNT(DISTINCT cluster_id) AS active_clusters
FROM cost_tracking.project_costs
GROUP BY project_id, usage_date, region;

-- Create view for recent anomalies
CREATE VIEW IF NOT EXISTS cost_tracking.recent_anomalies
AS
SELECT
  a.*,
  p.region,
  p.compute_type
FROM cost_tracking.anomalies a
JOIN cost_tracking.project_costs p 
  ON a.project_id = p.project_id 
  AND a.usage_date = p.usage_date
WHERE a.usage_date >= current_date - interval '30 days'
  AND a.status = 'open';

-- Create view for cost trends
CREATE VIEW IF NOT EXISTS cost_tracking.cost_trends
AS
SELECT
  project_id,
  usage_date,
  region,
  total_cost,
  LAG(total_cost, 1) OVER (PARTITION BY project_id ORDER BY usage_date) AS prev_day_cost,
  LAG(total_cost, 7) OVER (PARTITION BY project_id ORDER BY usage_date) AS week_ago_cost,
  AVG(total_cost) OVER (PARTITION BY project_id ORDER BY usage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg
FROM cost_tracking.project_costs_daily_summary
WHERE usage_date >= current_date - interval '30 days';

-- Insert sample workspace configuration
INSERT INTO cost_tracking.workspace_config 
(workspace_id, workspace_name, region, dbu_rate_all_purpose, dbu_rate_jobs, dbu_rate_sql_warehouse)
VALUES
('adb-1234567890123456.7.azuredatabricks.net', 'WestUS-Workspace', 'West US', 0.40, 0.40, 0.40),
('adb-9876543210987654.7.azuredatabricks.net', 'NorthEurope-Workspace', 'North Europe', 0.45, 0.45, 0.45),
('adb-5555666677778888.7.azuredatabricks.net', 'EastUS-Workspace', 'East US', 0.40, 0.40, 0.40);

-- Insert sample user permissions
INSERT INTO cost_tracking.user_permissions 
(user_id, project_id, permission_level, granted_by)
VALUES
('user1@company.com', 'ProjectA', 'read', 'admin@company.com'),
('user2@company.com', 'ProjectB', 'read', 'admin@company.com'),
('user3@company.com', 'ProjectC', 'read', 'admin@company.com'),
('admin@company.com', 'ProjectA', 'admin', 'system'),
('admin@company.com', 'ProjectB', 'admin', 'system'),
('admin@company.com', 'ProjectC', 'admin', 'system'); 
