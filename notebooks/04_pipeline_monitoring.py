"""
Pipeline Monitoring and Health Tracking
======================================
This notebook provides comprehensive monitoring for the cost tracking pipeline,
including health metrics, performance tracking, and automated alerting.

Features:
- Pipeline execution monitoring
- Performance metrics tracking
- Automated alerting for failures
- Health dashboard generation
- Cost optimization monitoring
- SLA compliance tracking
"""

import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
import json
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineMonitor:
    """Comprehensive pipeline monitoring and alerting system"""
    
    def __init__(self):
        self.alert_thresholds = {
            "success_rate": 0.95,  # 95% success rate
            "avg_execution_time": 300,  # 5 minutes
            "max_execution_time": 900,  # 15 minutes
            "error_rate": 0.05,  # 5% error rate
            "data_freshness_hours": 2,  # Data should be fresh within 2 hours
            "cost_threshold": 1000  # Alert if daily cost exceeds $1000
        }
    
    def get_pipeline_health(self, days=7):
        """Get overall pipeline health metrics"""
        try:
            # Get recent pipeline executions
            pipeline_logs = spark.sql(f"""
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
            WHERE start_time >= current_date - interval '{days} days'
            """)
            
            if pipeline_logs.count() == 0:
                logger.warning("No pipeline logs found for the specified period")
                return None
            
            # Calculate health metrics
            health_metrics = pipeline_logs.groupBy("pipeline_name").agg(
                count("*").alias("total_executions"),
                sum("success_score").alias("successful_executions"),
                avg("execution_time_seconds").alias("avg_execution_time"),
                max("execution_time_seconds").alias("max_execution_time"),
                sum("rows_processed").alias("total_rows_processed"),
                max("start_time").alias("last_execution"),
                count(when(col("error_message").isNotNull(), 1)).alias("error_count")
            ).withColumn(
                "success_rate",
                col("successful_executions") / col("total_executions")
            ).withColumn(
                "error_rate",
                col("error_count") / col("total_executions")
            ).withColumn(
                "health_status",
                when(col("success_rate") >= self.alert_thresholds["success_rate"], "healthy")
                .when(col("success_rate") >= 0.8, "warning")
                .otherwise("critical")
            )
            
            return health_metrics
            
        except Exception as e:
            logger.error(f"Error getting pipeline health: {str(e)}")
            return None
    
    def check_data_freshness(self):
        """Check if data is fresh and up-to-date"""
        try:
            # Check last update time for each table
            freshness_checks = {}
            
            # Check project_costs table
            last_cost_update = spark.sql("""
            SELECT MAX(updated_at) as last_update
            FROM cost_tracking.project_costs
            """).collect()[0]["last_update"]
            
            if last_cost_update:
                hours_since_update = (datetime.now() - last_cost_update).total_seconds() / 3600
                freshness_checks["project_costs"] = {
                    "last_update": last_cost_update,
                    "hours_since_update": hours_since_update,
                    "is_fresh": hours_since_update <= self.alert_thresholds["data_freshness_hours"]
                }
            
            # Check anomalies table
            last_anomaly_update = spark.sql("""
            SELECT MAX(detected_at) as last_update
            FROM cost_tracking.anomalies
            """).collect()[0]["last_update"]
            
            if last_anomaly_update:
                hours_since_update = (datetime.now() - last_anomaly_update).total_seconds() / 3600
                freshness_checks["anomalies"] = {
                    "last_update": last_anomaly_update,
                    "hours_since_update": hours_since_update,
                    "is_fresh": hours_since_update <= self.alert_thresholds["data_freshness_hours"]
                }
            
            # Check forecasts table
            last_forecast_update = spark.sql("""
            SELECT MAX(created_at) as last_update
            FROM cost_tracking.forecasts
            """).collect()[0]["last_update"]
            
            if last_forecast_update:
                hours_since_update = (datetime.now() - last_forecast_update).total_seconds() / 3600
                freshness_checks["forecasts"] = {
                    "last_update": last_forecast_update,
                    "hours_since_update": hours_since_update,
                    "is_fresh": hours_since_update <= self.alert_thresholds["data_freshness_hours"]
                }
            
            return freshness_checks
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            return {}
    
    def check_cost_thresholds(self):
        """Check if costs exceed thresholds"""
        try:
            # Get recent costs
            recent_costs = spark.sql("""
            SELECT 
                project_id,
                SUM(total_cost) as daily_cost,
                usage_date
            FROM cost_tracking.project_costs
            WHERE usage_date = current_date - interval '1 day'
            GROUP BY project_id, usage_date
            """)
            
            # Check for projects exceeding threshold
            high_cost_projects = recent_costs.filter(
                col("daily_cost") > self.alert_thresholds["cost_threshold"]
            )
            
            return high_cost_projects.collect()
            
        except Exception as e:
            logger.error(f"Error checking cost thresholds: {str(e)}")
            return []
    
    def generate_alerts(self):
        """Generate alerts based on monitoring results"""
        alerts = []
        
        try:
            # Check pipeline health
            health_metrics = self.get_pipeline_health()
            if health_metrics:
                for row in health_metrics.collect():
                    if row["health_status"] == "critical":
                        alerts.append({
                            "alert_id": str(uuid.uuid4()),
                            "alert_type": "pipeline_failure",
                            "severity": "critical",
                            "message": f"Pipeline {row['pipeline_name']} has critical health status. Success rate: {row['success_rate']:.2%}",
                            "details": {
                                "pipeline_name": row["pipeline_name"],
                                "success_rate": row["success_rate"],
                                "error_count": row["error_count"],
                                "last_execution": row["last_execution"]
                            },
                            "created_at": datetime.now()
                        })
                    elif row["health_status"] == "warning":
                        alerts.append({
                            "alert_id": str(uuid.uuid4()),
                            "alert_type": "pipeline_warning",
                            "severity": "warning",
                            "message": f"Pipeline {row['pipeline_name']} has warning health status. Success rate: {row['success_rate']:.2%}",
                            "details": {
                                "pipeline_name": row["pipeline_name"],
                                "success_rate": row["success_rate"],
                                "error_count": row["error_count"]
                            },
                            "created_at": datetime.now()
                        })
            
            # Check data freshness
            freshness_checks = self.check_data_freshness()
            for table_name, freshness_info in freshness_checks.items():
                if not freshness_info["is_fresh"]:
                    alerts.append({
                        "alert_id": str(uuid.uuid4()),
                        "alert_type": "data_staleness",
                        "severity": "warning",
                        "message": f"Table {table_name} data is stale. Last update: {freshness_info['hours_since_update']:.1f} hours ago",
                        "details": freshness_info,
                        "created_at": datetime.now()
                    })
            
            # Check cost thresholds
            high_cost_projects = self.check_cost_thresholds()
            for project in high_cost_projects:
                alerts.append({
                    "alert_id": str(uuid.uuid4()),
                    "alert_type": "cost_threshold_exceeded",
                    "severity": "warning",
                    "message": f"Project {project['project_id']} exceeded cost threshold. Daily cost: ${project['daily_cost']:.2f}",
                    "details": {
                        "project_id": project["project_id"],
                        "daily_cost": project["daily_cost"],
                        "threshold": self.alert_thresholds["cost_threshold"]
                    },
                    "created_at": datetime.now()
                })
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error generating alerts: {str(e)}")
            return []
    
    def log_alerts(self, alerts):
        """Log alerts to the system"""
        if not alerts:
            return
        
        try:
            # Convert alerts to DataFrame
            alerts_df = spark.createDataFrame(alerts)
            
            # Save to alerts table (if it exists)
            alerts_df.write.format("delta").mode("append").saveAsTable("cost_tracking.pipeline_alerts")
            
            logger.info(f"Logged {len(alerts)} alerts")
            
        except Exception as e:
            logger.error(f"Error logging alerts: {str(e)}")
    
    def get_performance_metrics(self, days=30):
        """Get detailed performance metrics"""
        try:
            # Get execution metrics
            execution_metrics = spark.sql(f"""
            SELECT 
                pipeline_name,
                DATE(start_time) as execution_date,
                COUNT(*) as execution_count,
                AVG(execution_time_seconds) as avg_execution_time,
                MAX(execution_time_seconds) as max_execution_time,
                MIN(execution_time_seconds) as min_execution_time,
                SUM(rows_processed) as total_rows_processed,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_executions,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_executions,
                COUNT(CASE WHEN status = 'partial' THEN 1 END) as partial_executions
            FROM cost_tracking.pipeline_logs
            WHERE start_time >= current_date - interval '{days} days'
            GROUP BY pipeline_name, DATE(start_time)
            ORDER BY pipeline_name, execution_date
            """)
            
            return execution_metrics
            
        except Exception as e:
            logger.error(f"Error getting performance metrics: {str(e)}")
            return None
    
    def generate_health_report(self):
        """Generate comprehensive health report"""
        try:
            report = {
                "report_id": str(uuid.uuid4()),
                "generated_at": datetime.now().isoformat(),
                "pipeline_health": {},
                "data_freshness": {},
                "cost_monitoring": {},
                "alerts": [],
                "recommendations": []
            }
            
            # Pipeline health
            health_metrics = self.get_pipeline_health()
            if health_metrics:
                report["pipeline_health"] = {
                    "overall_status": "healthy",
                    "pipelines": []
                }
                
                for row in health_metrics.collect():
                    pipeline_info = {
                        "name": row["pipeline_name"],
                        "status": row["health_status"],
                        "success_rate": float(row["success_rate"]),
                        "total_executions": row["total_executions"],
                        "error_count": row["error_count"],
                        "avg_execution_time": float(row["avg_execution_time"]),
                        "last_execution": row["last_execution"].isoformat() if row["last_execution"] else None
                    }
                    report["pipeline_health"]["pipelines"].append(pipeline_info)
                    
                    if row["health_status"] == "critical":
                        report["pipeline_health"]["overall_status"] = "critical"
                    elif row["health_status"] == "warning" and report["pipeline_health"]["overall_status"] == "healthy":
                        report["pipeline_health"]["overall_status"] = "warning"
            
            # Data freshness
            freshness_checks = self.check_data_freshness()
            report["data_freshness"] = {
                "overall_status": "fresh",
                "tables": freshness_checks
            }
            
            for table_name, freshness_info in freshness_checks.items():
                if not freshness_info["is_fresh"]:
                    report["data_freshness"]["overall_status"] = "stale"
            
            # Cost monitoring
            high_cost_projects = self.check_cost_thresholds()
            report["cost_monitoring"] = {
                "threshold_exceeded": len(high_cost_projects) > 0,
                "high_cost_projects": [
                    {
                        "project_id": project["project_id"],
                        "daily_cost": float(project["daily_cost"])
                    }
                    for project in high_cost_projects
                ]
            }
            
            # Generate alerts
            alerts = self.generate_alerts()
            report["alerts"] = [
                {
                    "type": alert["alert_type"],
                    "severity": alert["severity"],
                    "message": alert["message"]
                }
                for alert in alerts
            ]
            
            # Generate recommendations
            recommendations = self.generate_recommendations(report)
            report["recommendations"] = recommendations
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating health report: {str(e)}")
            return None
    
    def generate_recommendations(self, health_report):
        """Generate recommendations based on health report"""
        recommendations = []
        
        try:
            # Pipeline recommendations
            for pipeline in health_report.get("pipeline_health", {}).get("pipelines", []):
                if pipeline["status"] == "critical":
                    recommendations.append({
                        "type": "pipeline_optimization",
                        "priority": "high",
                        "title": f"Fix Critical Pipeline: {pipeline['name']}",
                        "description": f"Pipeline {pipeline['name']} has critical health status with {pipeline['success_rate']:.1%} success rate",
                        "action": "Review pipeline logs and fix underlying issues"
                    })
                elif pipeline["avg_execution_time"] > self.alert_thresholds["avg_execution_time"]:
                    recommendations.append({
                        "type": "performance_optimization",
                        "priority": "medium",
                        "title": f"Optimize Pipeline Performance: {pipeline['name']}",
                        "description": f"Pipeline {pipeline['name']} has high average execution time: {pipeline['avg_execution_time']:.1f} seconds",
                        "action": "Review query optimization and resource allocation"
                    })
            
            # Data freshness recommendations
            if health_report.get("data_freshness", {}).get("overall_status") == "stale":
                recommendations.append({
                    "type": "data_freshness",
                    "priority": "high",
                    "title": "Address Data Staleness",
                    "description": "Some tables have stale data that may affect dashboard accuracy",
                    "action": "Check pipeline schedules and ensure timely execution"
                })
            
            # Cost optimization recommendations
            if health_report.get("cost_monitoring", {}).get("threshold_exceeded"):
                recommendations.append({
                    "type": "cost_optimization",
                    "priority": "medium",
                    "title": "Review High-Cost Projects",
                    "description": "Some projects are exceeding cost thresholds",
                    "action": "Review resource usage and implement cost controls"
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            return []

# Main monitoring execution
def run_pipeline_monitoring():
    """Run the complete pipeline monitoring process"""
    logger.info("Starting pipeline monitoring")
    
    try:
        monitor = PipelineMonitor()
        
        # Generate health report
        health_report = monitor.generate_health_report()
        
        if health_report:
            # Log the report
            logger.info("Health Report Generated:")
            logger.info(json.dumps(health_report, indent=2, default=str))
            
            # Generate and log alerts
            alerts = monitor.generate_alerts()
            if alerts:
                monitor.log_alerts(alerts)
                logger.info(f"Generated {len(alerts)} alerts")
            
            # Save health report to storage
            health_report_df = spark.createDataFrame([health_report])
            health_report_df.write.format("delta").mode("append").saveAsTable("cost_tracking.health_reports")
            
            logger.info("Pipeline monitoring completed successfully")
            
            return health_report
        else:
            logger.error("Failed to generate health report")
            return None
            
    except Exception as e:
        logger.error(f"Error in pipeline monitoring: {str(e)}")
        return None

# Dashboard utility functions
def display_monitoring_dashboard():
    """Display monitoring dashboard components"""
    print("=== Pipeline Monitoring Dashboard ===")
    
    monitor = PipelineMonitor()
    
    # Pipeline health
    health_metrics = monitor.get_pipeline_health()
    if health_metrics:
        print("\nPipeline Health Status:")
        health_metrics.show()
    
    # Data freshness
    freshness_checks = monitor.check_data_freshness()
    print("\nData Freshness Status:")
    for table_name, freshness_info in freshness_checks.items():
        status = "✅ FRESH" if freshness_info["is_fresh"] else "⚠️ STALE"
        print(f"  {table_name}: {status} (Last update: {freshness_info['hours_since_update']:.1f} hours ago)")
    
    # Cost monitoring
    high_cost_projects = monitor.check_cost_thresholds()
    if high_cost_projects:
        print(f"\n⚠️ High Cost Projects ({len(high_cost_projects)}):")
        for project in high_cost_projects:
            print(f"  {project['project_id']}: ${project['daily_cost']:.2f}")
    else:
        print("\n✅ All projects within cost thresholds")
    
    # Recent alerts
    alerts = monitor.generate_alerts()
    if alerts:
        print(f"\n🚨 Recent Alerts ({len(alerts)}):")
        for alert in alerts[:5]:  # Show top 5 alerts
            print(f"  [{alert['severity'].upper()}] {alert['message']}")
    else:
        print("\n✅ No active alerts")

def get_performance_trends():
    """Get performance trends for visualization"""
    monitor = PipelineMonitor()
    return monitor.get_performance_metrics()

# Run monitoring
if __name__ == "__main__":
    # Run the monitoring process
    health_report = run_pipeline_monitoring()
    
    # Display dashboard
    display_monitoring_dashboard()
    
    # Show performance trends
    performance_metrics = get_performance_trends()
    if performance_metrics:
        print("\nPerformance Trends (Last 30 days):")
        performance_metrics.show() 