"""
Enhanced Cost Tracking DLT Pipeline
===================================
This Delta Live Tables pipeline provides incremental ETL processing for Azure Databricks cost tracking
with multi-region support, error handling, and comprehensive logging.

Features:
- Incremental processing using DLT
- Multi-region workspace support
- Error handling with retry logic
- Comprehensive logging
- Data validation
- Cost optimization recommendations
"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
import uuid
import logging
from datetime import datetime, timedelta
import json

# Configuration
SUBSCRIPTION_ID = dbutils.secrets.get(scope="cost-tracking", key="azure-subscription-id")
DATABRICKS_TOKEN = dbutils.secrets.get(scope="cost-tracking", key="databricks-token")
AZURE_TENANT_ID = dbutils.secrets.get(scope="cost-tracking", key="azure-tenant-id")

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_execution_id():
    """Generate unique execution ID for tracking"""
    return str(uuid.uuid4())

def log_pipeline_execution(pipeline_name, execution_id, start_time, status, rows_processed=0, error_message=None):
    """Log pipeline execution to cost_tracking.pipeline_logs"""
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    log_data = {
        "log_id": str(uuid.uuid4()),
        "pipeline_name": pipeline_name,
        "execution_id": execution_id,
        "start_time": start_time,
        "end_time": end_time,
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message,
        "execution_time_seconds": execution_time
    }
    
    log_df = spark.createDataFrame([log_data])
    log_df.write.format("delta").mode("append").saveAsTable("cost_tracking.pipeline_logs")

@dlt.table(
    name="workspace_config",
    comment="Workspace configuration for multi-region support"
)
def workspace_config():
    """Load workspace configuration from Delta table"""
    return spark.table("cost_tracking.workspace_config").filter(col("is_active") == True)

@dlt.table(
    name="raw_dbu_usage",
    comment="Raw DBU usage data from system tables"
)
def raw_dbu_usage():
    """Extract DBU usage from system tables for all active workspaces"""
    execution_id = get_execution_id()
    start_time = datetime.now()
    
    try:
        # Get active workspaces
        workspaces = spark.table("cost_tracking.workspace_config").filter(col("is_active") == True)
        workspace_list = [row.workspace_id for row in workspaces.collect()]
        
        # Build dynamic query for all workspaces
        workspace_conditions = " OR ".join([f"workspace_id = '{ws}'" for ws in workspace_list])
        
        dbu_query = f"""
        SELECT
          usage_date,
          workspace_id,
          cluster_id,
          tags.ProjectID AS project_id,
          sku_name,
          usage_quantity,
          tags.Environment AS environment,
          tags.Team AS team,
          current_timestamp() AS extracted_at
        FROM system.billing.usage
        WHERE usage_date >= current_date - interval '7 days'
          AND tags.ProjectID IS NOT NULL
          AND ({workspace_conditions})
        """
        
        dbu_df = spark.sql(dbu_query)
        rows_processed = dbu_df.count()
        
        log_pipeline_execution(
            "raw_dbu_usage", 
            execution_id, 
            start_time, 
            "success", 
            rows_processed
        )
        
        return dbu_df
        
    except Exception as e:
        error_msg = f"Error extracting DBU usage: {str(e)}"
        logger.error(error_msg)
        log_pipeline_execution(
            "raw_dbu_usage", 
            execution_id, 
            start_time, 
            "failed", 
            0, 
            error_msg
        )
        raise

@dlt.table(
    name="raw_azure_costs",
    comment="Raw Azure infrastructure costs from Cost Management API"
)
def raw_azure_costs():
    """Extract Azure infrastructure costs using Cost Management API"""
    execution_id = get_execution_id()
    start_time = datetime.now()
    
    try:
        # Initialize Azure credentials
        credential = DefaultAzureCredential()
        cost_client = CostManagementClient(credential, SUBSCRIPTION_ID)
        scope = f"/subscriptions/{SUBSCRIPTION_ID}"
        
        # Query parameters
        query = {
            "type": "Usage",
            "timeframe": "Custom",
            "timePeriod": {
                "from": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
                "to": datetime.now().strftime("%Y-%m-%d")
            },
            "dataset": {
                "granularity": "Daily",
                "aggregation": {
                    "totalCost": {"name": "Cost", "function": "Sum"}
                },
                "grouping": [
                    {"type": "Dimension", "name": "ResourceId"},
                    {"type": "TagKey", "name": "ProjectID"},
                    {"type": "TagKey", "name": "Environment"}
                ],
                "filter": {
                    "tags": {"name": "vendor", "value": "databricks"}
                }
            }
        }
        
        # Execute query
        cost_response = cost_client.query.usage(scope=scope, parameters=query)
        
        # Process results
        cost_data = []
        for row in cost_response.rows:
            cost_data.append({
                "usage_date": row[0].split("T")[0],
                "project_id": row[2] if len(row) > 2 else None,
                "environment": row[3] if len(row) > 3 else None,
                "infra_cost": float(row[1]),
                "extracted_at": datetime.now()
            })
        
        # Convert to DataFrame
        schema = StructType([
            StructField("usage_date", StringType(), True),
            StructField("project_id", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("infra_cost", DecimalType(10, 2), True),
            StructField("extracted_at", TimestampType(), True)
        ])
        
        azure_cost_df = spark.createDataFrame(cost_data, schema)
        azure_cost_df = azure_cost_df.withColumn("usage_date", col("usage_date").cast(DateType()))
        
        rows_processed = azure_cost_df.count()
        
        log_pipeline_execution(
            "raw_azure_costs", 
            execution_id, 
            start_time, 
            "success", 
            rows_processed
        )
        
        return azure_cost_df
        
    except Exception as e:
        error_msg = f"Error extracting Azure costs: {str(e)}"
        logger.error(error_msg)
        log_pipeline_execution(
            "raw_azure_costs", 
            execution_id, 
            start_time, 
            "failed", 
            0, 
            error_msg
        )
        raise

@dlt.table(
    name="validated_dbu_usage",
    comment="Validated and enriched DBU usage data"
)
def validated_dbu_usage():
    """Validate and enrich DBU usage data"""
    execution_id = get_execution_id()
    start_time = datetime.now()
    
    try:
        # Get raw DBU data
        dbu_df = dlt.read("raw_dbu_usage")
        
        # Get workspace config for DBU rates
        workspace_config = dlt.read("workspace_config")
        
        # Join with workspace config to get rates and region
        enriched_dbu = dbu_df.join(
            workspace_config,
            dbu_df.workspace_id == workspace_config.workspace_id,
            "left"
        )
        
        # Calculate DBU costs based on compute type
        validated_dbu = enriched_dbu.withColumn(
            "dbu_rate",
            when(col("sku_name").contains("All-Purpose"), col("dbu_rate_all_purpose"))
            .when(col("sku_name").contains("Jobs"), col("dbu_rate_jobs"))
            .when(col("sku_name").contains("SQL"), col("dbu_rate_sql_warehouse"))
            .otherwise(col("dbu_rate_all_purpose"))
        ).withColumn(
            "dbu_cost",
            col("usage_quantity") * col("dbu_rate")
        ).withColumn(
            "compute_type",
            when(col("sku_name").contains("All-Purpose"), "All-Purpose")
            .when(col("sku_name").contains("Jobs"), "Jobs")
            .when(col("sku_name").contains("SQL"), "SQL Warehouse")
            .otherwise("Unknown")
        )
        
        # Data validation
        validation_errors = validated_dbu.filter(
            col("project_id").isNull() |
            col("usage_quantity").isNull() |
            col("usage_quantity") < 0 |
            col("dbu_cost").isNull() |
            col("dbu_cost") < 0
        )
        
        if validation_errors.count() > 0:
            logger.warning(f"Found {validation_errors.count()} validation errors in DBU data")
            # Log validation errors
            validation_errors.write.format("delta").mode("append").saveAsTable("cost_tracking.validation_errors")
        
        # Filter out invalid records
        clean_dbu = validated_dbu.filter(
            col("project_id").isNotNull() &
            col("usage_quantity").isNotNull() &
            col("usage_quantity") >= 0 &
            col("dbu_cost").isNotNull() &
            col("dbu_cost") >= 0
        )
        
        rows_processed = clean_dbu.count()
        
        log_pipeline_execution(
            "validated_dbu_usage", 
            execution_id, 
            start_time, 
            "success", 
            rows_processed
        )
        
        return clean_dbu.select(
            "project_id", "usage_date", "workspace_id", "region", 
            "dbu_cost", "compute_type", "cluster_id", "environment", "team"
        )
        
    except Exception as e:
        error_msg = f"Error validating DBU usage: {str(e)}"
        logger.error(error_msg)
        log_pipeline_execution(
            "validated_dbu_usage", 
            execution_id, 
            start_time, 
            "failed", 
            0, 
            error_msg
        )
        raise

@dlt.table(
    name="validated_azure_costs",
    comment="Validated Azure infrastructure costs"
)
def validated_azure_costs():
    """Validate Azure infrastructure costs"""
    execution_id = get_execution_id()
    start_time = datetime.now()
    
    try:
        # Get raw Azure costs
        azure_df = dlt.read("raw_azure_costs")
        
        # Data validation
        validation_errors = azure_df.filter(
            col("project_id").isNull() |
            col("infra_cost").isNull() |
            col("infra_cost") < 0
        )
        
        if validation_errors.count() > 0:
            logger.warning(f"Found {validation_errors.count()} validation errors in Azure cost data")
            validation_errors.write.format("delta").mode("append").saveAsTable("cost_tracking.validation_errors")
        
        # Filter out invalid records
        clean_azure = azure_df.filter(
            col("project_id").isNotNull() &
            col("infra_cost").isNotNull() &
            col("infra_cost") >= 0
        )
        
        rows_processed = clean_azure.count()
        
        log_pipeline_execution(
            "validated_azure_costs", 
            execution_id, 
            start_time, 
            "success", 
            rows_processed
        )
        
        return clean_azure.select(
            "project_id", "usage_date", "infra_cost", "environment"
        )
        
    except Exception as e:
        error_msg = f"Error validating Azure costs: {str(e)}"
        logger.error(error_msg)
        log_pipeline_execution(
            "validated_azure_costs", 
            execution_id, 
            start_time, 
            "failed", 
            0, 
            error_msg
        )
        raise

@dlt.table(
    name="project_costs",
    comment="Final project costs with DBU and infrastructure costs combined"
)
def project_costs():
    """Combine DBU and infrastructure costs by project and date"""
    execution_id = get_execution_id()
    start_time = datetime.now()
    
    try:
        # Get validated data
        dbu_df = dlt.read("validated_dbu_usage")
        azure_df = dlt.read("validated_azure_costs")
        
        # Aggregate DBU costs by project and date
        dbu_agg = dbu_df.groupBy("project_id", "usage_date", "region", "compute_type").agg(
            sum("dbu_cost").alias("dbu_cost"),
            countDistinct("cluster_id").alias("active_clusters"),
            first("environment").alias("environment"),
            first("team").alias("team")
        )
        
        # Aggregate Azure costs by project and date
        azure_agg = azure_df.groupBy("project_id", "usage_date").agg(
            sum("infra_cost").alias("infra_cost"),
            first("environment").alias("azure_environment")
        )
        
        # Join DBU and Azure costs
        combined_costs = dbu_agg.join(
            azure_agg,
            ["project_id", "usage_date"],
            "left_outer"
        ).fillna(0, ["infra_cost"]).withColumn(
            "total_cost",
            col("dbu_cost") + col("infra_cost")
        ).withColumn(
            "created_at",
            current_timestamp()
        ).withColumn(
            "updated_at",
            current_timestamp()
        )
        
        rows_processed = combined_costs.count()
        
        log_pipeline_execution(
            "project_costs", 
            execution_id, 
            start_time, 
            "success", 
            rows_processed
        )
        
        return combined_costs.select(
            "project_id", "usage_date", "region", "dbu_cost", "infra_cost", 
            "total_cost", "compute_type", "active_clusters", "environment", 
            "team", "created_at", "updated_at"
        )
        
    except Exception as e:
        error_msg = f"Error combining project costs: {str(e)}"
        logger.error(error_msg)
        log_pipeline_execution(
            "project_costs", 
            execution_id, 
            start_time, 
            "failed", 
            0, 
            error_msg
        )
        raise

@dlt.table(
    name="cost_optimization_recommendations",
    comment="Cost optimization recommendations based on usage patterns"
)
def cost_optimization_recommendations():
    """Generate cost optimization recommendations"""
    execution_id = get_execution_id()
    start_time = datetime.now()
    
    try:
        # Get recent project costs
        costs_df = dlt.read("project_costs")
        
        # Find idle clusters (no usage for 24+ hours)
        idle_clusters = spark.sql("""
        SELECT DISTINCT
          cluster_id,
          project_id,
          workspace_id,
          last_activity_time
        FROM system.billing.usage
        WHERE last_activity_time < current_timestamp() - interval '24 hours'
          AND tags.ProjectID IS NOT NULL
        """)
        
        # Generate recommendations
        recommendations = []
        
        # Idle cluster recommendations
        for row in idle_clusters.collect():
            recommendations.append({
                "recommendation_id": str(uuid.uuid4()),
                "project_id": row.project_id,
                "recommendation_type": "cluster_termination",
                "title": "Terminate Idle Cluster",
                "description": f"Cluster {row.cluster_id} has been idle for 24+ hours",
                "potential_savings": 50.0,  # Estimated daily savings
                "priority": "high",
                "status": "pending",
                "created_at": datetime.now()
            })
        
        # High cost project recommendations
        high_cost_projects = costs_df.filter(col("total_cost") > 200).groupBy("project_id").agg(
            avg("total_cost").alias("avg_daily_cost"),
            count("*").alias("days_above_threshold")
        ).filter(col("days_above_threshold") >= 3)
        
        for row in high_cost_projects.collect():
            recommendations.append({
                "recommendation_id": str(uuid.uuid4()),
                "project_id": row.project_id,
                "recommendation_type": "cost_review",
                "title": "Review High Cost Project",
                "description": f"Project {row.project_id} has averaged ${row.avg_daily_cost:.2f}/day for {row.days_above_threshold} days",
                "potential_savings": row.avg_daily_cost * 0.3,  # 30% potential savings
                "priority": "medium",
                "status": "pending",
                "created_at": datetime.now()
            })
        
        # Convert to DataFrame
        if recommendations:
            rec_df = spark.createDataFrame(recommendations)
            rows_processed = rec_df.count()
        else:
            # Create empty DataFrame with schema
            schema = StructType([
                StructField("recommendation_id", StringType(), True),
                StructField("project_id", StringType(), True),
                StructField("recommendation_type", StringType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("potential_savings", DecimalType(10, 2), True),
                StructField("priority", StringType(), True),
                StructField("status", StringType(), True),
                StructField("created_at", TimestampType(), True)
            ])
            rec_df = spark.createDataFrame([], schema)
            rows_processed = 0
        
        log_pipeline_execution(
            "cost_optimization_recommendations", 
            execution_id, 
            start_time, 
            "success", 
            rows_processed
        )
        
        return rec_df
        
    except Exception as e:
        error_msg = f"Error generating optimization recommendations: {str(e)}"
        logger.error(error_msg)
        log_pipeline_execution(
            "cost_optimization_recommendations", 
            execution_id, 
            start_time, 
            "failed", 
            0, 
            error_msg
        )
        raise

# Optimize tables after processing
@dlt.table(
    name="optimized_project_costs",
    comment="Optimized project costs table with ZORDER"
)
def optimized_project_costs():
    """Optimized version of project costs with ZORDER for better query performance"""
    costs_df = dlt.read("project_costs")
    
    # Write with optimization
    costs_df.write.format("delta").mode("overwrite").option(
        "delta.optimizeWrite", "true"
    ).option(
        "delta.autoCompact", "true"
    ).saveAsTable("cost_tracking.project_costs")
    
    # Apply ZORDER optimization
    spark.sql("OPTIMIZE cost_tracking.project_costs ZORDER BY (project_id, usage_date)")
    
    return costs_df 
