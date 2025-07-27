-- Security Configuration for Cost Tracking System
-- ===============================================
-- This file contains Unity Catalog security setup, row-level security,
-- audit logging, and access control configurations.

-- 1. UNITY CATALOG PERMISSIONS
-- ============================

-- Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS cost_tracking_catalog;

-- Create schema with proper permissions
CREATE SCHEMA IF NOT EXISTS cost_tracking_catalog.cost_tracking;

-- Grant catalog permissions to admin users
GRANT ALL PRIVILEGES ON CATALOG cost_tracking_catalog TO `admin@company.com`;

-- Grant schema permissions
GRANT ALL PRIVILEGES ON SCHEMA cost_tracking_catalog.cost_tracking TO `admin@company.com`;

-- 2. ROW-LEVEL SECURITY (RLS) SETUP
-- ==================================

-- Create RLS policies for project-based access control
-- Users can only see data for projects they have access to

-- RLS for project_costs table
CREATE ROW ACCESS POLICY IF NOT EXISTS project_costs_rls
ON cost_tracking.project_costs
AS (project_id)
USING (
    project_id IN (
        SELECT project_id 
        FROM cost_tracking.user_permissions 
        WHERE user_id = current_user() 
          AND is_active = true
    )
    OR current_user() IN (
        SELECT user_id 
        FROM cost_tracking.user_permissions 
        WHERE permission_level = 'admin' 
          AND is_active = true
    )
);

-- RLS for anomalies table
CREATE ROW ACCESS POLICY IF NOT EXISTS anomalies_rls
ON cost_tracking.anomalies
AS (project_id)
USING (
    project_id IN (
        SELECT project_id 
        FROM cost_tracking.user_permissions 
        WHERE user_id = current_user() 
          AND is_active = true
    )
    OR current_user() IN (
        SELECT user_id 
        FROM cost_tracking.user_permissions 
        WHERE permission_level = 'admin' 
          AND is_active = true
    )
);

-- RLS for forecasts table
CREATE ROW ACCESS POLICY IF NOT EXISTS forecasts_rls
ON cost_tracking.forecasts
AS (project_id)
USING (
    project_id IN (
        SELECT project_id 
        FROM cost_tracking.user_permissions 
        WHERE user_id = current_user() 
          AND is_active = true
    )
    OR current_user() IN (
        SELECT user_id 
        FROM cost_tracking.user_permissions 
        WHERE permission_level = 'admin' 
          AND is_active = true
    )
);

-- RLS for optimization_recommendations table
CREATE ROW ACCESS POLICY IF NOT EXISTS optimization_recommendations_rls
ON cost_tracking.optimization_recommendations
AS (project_id)
USING (
    project_id IN (
        SELECT project_id 
        FROM cost_tracking.user_permissions 
        WHERE user_id = current_user() 
          AND is_active = true
    )
    OR current_user() IN (
        SELECT user_id 
        FROM cost_tracking.user_permissions 
        WHERE permission_level = 'admin' 
          AND is_active = true
    )
);

-- 3. USER PERMISSIONS MANAGEMENT
-- ==============================

-- Function to grant project access to users
CREATE OR REPLACE FUNCTION cost_tracking.grant_project_access(
    target_user STRING,
    project_id STRING,
    permission_level STRING DEFAULT 'read',
    granted_by STRING DEFAULT current_user()
)
RETURNS STRING
AS $$
BEGIN
    -- Check if grantor has admin permissions
    IF NOT EXISTS (
        SELECT 1 FROM cost_tracking.user_permissions 
        WHERE user_id = granted_by 
          AND permission_level = 'admin' 
          AND is_active = true
    ) THEN
        RETURN 'ERROR: Insufficient permissions to grant access';
    END IF;
    
    -- Insert or update user permission
    MERGE INTO cost_tracking.user_permissions AS target
    USING (
        SELECT 
            target_user AS user_id,
            project_id,
            permission_level,
            granted_by,
            true AS is_active
    ) AS source
    ON target.user_id = source.user_id AND target.project_id = source.project_id
    WHEN MATCHED THEN
        UPDATE SET 
            permission_level = source.permission_level,
            granted_by = source.granted_by,
            is_active = source.is_active
    WHEN NOT MATCHED THEN
        INSERT (user_id, project_id, permission_level, granted_by, is_active)
        VALUES (source.user_id, source.project_id, source.permission_level, source.granted_by, source.is_active);
    
    RETURN 'SUCCESS: Access granted to ' || target_user || ' for project ' || project_id;
END;
$$;

-- Function to revoke project access from users
CREATE OR REPLACE FUNCTION cost_tracking.revoke_project_access(
    target_user STRING,
    project_id STRING,
    revoked_by STRING DEFAULT current_user()
)
RETURNS STRING
AS $$
BEGIN
    -- Check if revoker has admin permissions
    IF NOT EXISTS (
        SELECT 1 FROM cost_tracking.user_permissions 
        WHERE user_id = revoked_by 
          AND permission_level = 'admin' 
          AND is_active = true
    ) THEN
        RETURN 'ERROR: Insufficient permissions to revoke access';
    END IF;
    
    -- Deactivate user permission
    UPDATE cost_tracking.user_permissions 
    SET is_active = false
    WHERE user_id = target_user 
      AND project_id = project_id;
    
    RETURN 'SUCCESS: Access revoked from ' || target_user || ' for project ' || project_id;
END;
$$;

-- 4. AUDIT LOGGING SETUP
-- =======================

-- Function to log dashboard access
CREATE OR REPLACE FUNCTION cost_tracking.log_dashboard_access(
    action STRING,
    resource_type STRING,
    resource_id STRING DEFAULT NULL,
    ip_address STRING DEFAULT NULL,
    user_agent STRING DEFAULT NULL
)
RETURNS STRING
AS $$
BEGIN
    INSERT INTO cost_tracking.audit_logs (
        audit_id,
        user_id,
        action,
        resource_type,
        resource_id,
        ip_address,
        user_agent,
        accessed_at
    ) VALUES (
        uuid(),
        current_user(),
        action,
        resource_type,
        resource_id,
        ip_address,
        user_agent,
        current_timestamp()
    );
    
    RETURN 'SUCCESS: Access logged';
END;
$$;

-- 5. SECURE VIEWS WITH AUDIT LOGGING
-- ===================================

-- Secure view for project costs with audit logging
CREATE OR REPLACE VIEW cost_tracking.secure_project_costs AS
SELECT 
    pc.*,
    cost_tracking.log_dashboard_access(
        'dashboard_view',
        'project_costs',
        pc.project_id,
        -- Note: In production, you would extract IP and user agent from session context
        NULL,
        NULL
    ) AS audit_logged
FROM cost_tracking.project_costs pc;

-- Secure view for anomalies with audit logging
CREATE OR REPLACE VIEW cost_tracking.secure_anomalies AS
SELECT 
    a.*,
    cost_tracking.log_dashboard_access(
        'dashboard_view',
        'anomalies',
        a.project_id,
        NULL,
        NULL
    ) AS audit_logged
FROM cost_tracking.anomalies a;

-- 6. DATA MASKING AND ENCRYPTION
-- ===============================

-- Mask sensitive data in audit logs
CREATE OR REPLACE VIEW cost_tracking.masked_audit_logs AS
SELECT 
    audit_id,
    CASE 
        WHEN user_id LIKE '%@%' THEN 
            CONCAT(SPLIT(user_id, '@')[0], '@***')
        ELSE user_id 
    END AS masked_user_id,
    action,
    resource_type,
    resource_id,
    CASE 
        WHEN ip_address IS NOT NULL THEN 
            CONCAT(SPLIT(ip_address, '.')[0], '.', SPLIT(ip_address, '.')[1], '.***.***')
        ELSE ip_address 
    END AS masked_ip_address,
    CASE 
        WHEN user_agent IS NOT NULL THEN 
            CONCAT(SPLIT(user_agent, ' ')[0], ' ***')
        ELSE user_agent 
    END AS masked_user_agent,
    accessed_at
FROM cost_tracking.audit_logs;

-- 7. SECURITY MONITORING QUERIES
-- ===============================

-- Recent access patterns
CREATE OR REPLACE VIEW cost_tracking.security_access_patterns AS
SELECT 
    user_id,
    action,
    resource_type,
    COUNT(*) AS access_count,
    MIN(accessed_at) AS first_access,
    MAX(accessed_at) AS last_access,
    COUNT(DISTINCT DATE(accessed_at)) AS unique_days
FROM cost_tracking.audit_logs
WHERE accessed_at >= current_date - interval '30 days'
GROUP BY user_id, action, resource_type
ORDER BY access_count DESC;

-- Suspicious activity detection
CREATE OR REPLACE VIEW cost_tracking.suspicious_activity AS
WITH user_activity AS (
    SELECT 
        user_id,
        DATE(accessed_at) AS access_date,
        HOUR(accessed_at) AS access_hour,
        COUNT(*) AS hourly_requests
    FROM cost_tracking.audit_logs
    WHERE accessed_at >= current_date - interval '7 days'
    GROUP BY user_id, DATE(accessed_at), HOUR(accessed_at)
),
anomalous_users AS (
    SELECT 
        user_id,
        access_date,
        access_hour,
        hourly_requests,
        AVG(hourly_requests) OVER (PARTITION BY user_id) AS avg_hourly_requests,
        STDDEV(hourly_requests) OVER (PARTITION BY user_id) AS stddev_hourly_requests
    FROM user_activity
)
SELECT 
    user_id,
    access_date,
    access_hour,
    hourly_requests,
    avg_hourly_requests,
    stddev_hourly_requests,
    CASE 
        WHEN hourly_requests > avg_hourly_requests + (2 * stddev_hourly_requests) THEN 'HIGH'
        WHEN hourly_requests > avg_hourly_requests + stddev_hourly_requests THEN 'MEDIUM'
        ELSE 'NORMAL'
    END AS activity_level
FROM anomalous_users
WHERE hourly_requests > avg_hourly_requests + stddev_hourly_requests
ORDER BY hourly_requests DESC;

-- 8. PERMISSION AUDIT TRAIL
-- ==========================

-- Track permission changes
CREATE OR REPLACE VIEW cost_tracking.permission_audit_trail AS
SELECT 
    user_id,
    project_id,
    permission_level,
    granted_at,
    granted_by,
    is_active,
    CASE 
        WHEN is_active = true THEN 'ACTIVE'
        ELSE 'REVOKED'
    END AS status
FROM cost_tracking.user_permissions
ORDER BY granted_at DESC;

-- 9. SECURITY COMPLIANCE CHECKS
-- ==============================

-- Check for orphaned permissions (users with access to non-existent projects)
CREATE OR REPLACE VIEW cost_tracking.orphaned_permissions AS
SELECT 
    up.user_id,
    up.project_id,
    up.permission_level,
    up.granted_at
FROM cost_tracking.user_permissions up
LEFT JOIN cost_tracking.project_costs pc 
    ON up.project_id = pc.project_id
WHERE pc.project_id IS NULL
  AND up.is_active = true;

-- Check for users with excessive permissions
CREATE OR REPLACE VIEW cost_tracking.excessive_permissions AS
SELECT 
    user_id,
    COUNT(DISTINCT project_id) AS project_count,
    COUNT(CASE WHEN permission_level = 'admin' THEN 1 END) AS admin_permissions,
    MAX(granted_at) AS last_permission_granted
FROM cost_tracking.user_permissions
WHERE is_active = true
GROUP BY user_id
HAVING COUNT(DISTINCT project_id) > 10 OR COUNT(CASE WHEN permission_level = 'admin' THEN 1 END) > 0;

-- 10. SECURITY CONFIGURATION VALIDATION
-- =====================================

-- Validate RLS policies are active
CREATE OR REPLACE VIEW cost_tracking.rls_policy_status AS
SELECT 
    'project_costs' AS table_name,
    'project_costs_rls' AS policy_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM cost_tracking.project_costs LIMIT 1
        ) THEN 'ACTIVE'
        ELSE 'NOT_APPLIED'
    END AS status
UNION ALL
SELECT 
    'anomalies' AS table_name,
    'anomalies_rls' AS policy_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM cost_tracking.anomalies LIMIT 1
        ) THEN 'ACTIVE'
        ELSE 'NOT_APPLIED'
    END AS status
UNION ALL
SELECT 
    'forecasts' AS table_name,
    'forecasts_rls' AS policy_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM cost_tracking.forecasts LIMIT 1
        ) THEN 'ACTIVE'
        ELSE 'NOT_APPLIED'
    END AS status;

-- 11. SECURITY ALERTS AND NOTIFICATIONS
-- =====================================

-- Function to generate security alerts
CREATE OR REPLACE FUNCTION cost_tracking.generate_security_alert(
    alert_type STRING,
    alert_message STRING,
    severity STRING DEFAULT 'medium'
)
RETURNS STRING
AS $$
BEGIN
    -- In production, this would integrate with your alerting system
    -- For now, we'll log to a security alerts table
    
    INSERT INTO cost_tracking.security_alerts (
        alert_id,
        alert_type,
        alert_message,
        severity,
        created_at,
        status
    ) VALUES (
        uuid(),
        alert_type,
        alert_message,
        severity,
        current_timestamp(),
        'open'
    );
    
    RETURN 'ALERT: ' || alert_type || ' - ' || alert_message;
END;
$$;

-- 12. SECURITY MAINTENANCE PROCEDURES
-- ====================================

-- Clean up old audit logs (keep 90 days)
CREATE OR REPLACE PROCEDURE cost_tracking.cleanup_old_audit_logs()
AS $$
BEGIN
    DELETE FROM cost_tracking.audit_logs 
    WHERE accessed_at < current_date - interval '90 days';
    
    -- Also clean up old security alerts
    DELETE FROM cost_tracking.security_alerts 
    WHERE created_at < current_date - interval '30 days'
      AND status = 'resolved';
END;
$$;

-- Rotate user permissions (deactivate old inactive permissions)
CREATE OR REPLACE PROCEDURE cost_tracking.rotate_user_permissions()
AS $$
BEGIN
    -- Deactivate permissions for users who haven't accessed the system in 90 days
    UPDATE cost_tracking.user_permissions 
    SET is_active = false
    WHERE user_id IN (
        SELECT DISTINCT user_id 
        FROM cost_tracking.audit_logs 
        WHERE accessed_at < current_date - interval '90 days'
    )
    AND permission_level != 'admin';
END;
$$;

-- 13. SECURITY REPORTING
-- =======================

-- Monthly security report
CREATE OR REPLACE VIEW cost_tracking.monthly_security_report AS
SELECT 
    DATE_TRUNC('month', accessed_at) AS report_month,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) AS total_access_events,
    COUNT(DISTINCT resource_type) AS resource_types_accessed,
    COUNT(CASE WHEN action = 'dashboard_view' THEN 1 END) AS dashboard_views,
    COUNT(CASE WHEN action = 'data_export' THEN 1 END) AS data_exports,
    COUNT(CASE WHEN action = 'anomaly_acknowledge' THEN 1 END) AS anomaly_acknowledgments
FROM cost_tracking.audit_logs
WHERE accessed_at >= current_date - interval '12 months'
GROUP BY DATE_TRUNC('month', accessed_at)
ORDER BY report_month DESC; 