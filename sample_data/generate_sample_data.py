"""
Sample Data Generator for Cost Tracking System
=============================================
This script generates realistic sample data for testing the cost tracking dashboard.
It creates sample projects, costs, anomalies, and forecasts to demonstrate the system.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import random

def generate_sample_data():
    """Generate comprehensive sample data for the cost tracking system"""
    
    # Configuration
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now() + timedelta(days=7)  # Include forecast period
    
    # Sample projects
    projects = [
        {"id": "ProjectA", "name": "Data Analytics Platform", "region": "West US", "team": "Data Science"},
        {"id": "ProjectB", "name": "Machine Learning Pipeline", "region": "North Europe", "team": "ML Engineering"},
        {"id": "ProjectC", "name": "ETL Processing System", "region": "East US", "team": "Data Engineering"},
        {"id": "ProjectD", "name": "Real-time Dashboard", "region": "West US", "team": "Frontend"},
        {"id": "ProjectE", "name": "API Gateway", "region": "North Europe", "team": "Backend"}
    ]
    
    # Sample workspaces
    workspaces = [
        {"id": "adb-1234567890123456.7.azuredatabricks.net", "name": "WestUS-Workspace", "region": "West US"},
        {"id": "adb-9876543210987654.7.azuredatabricks.net", "name": "NorthEurope-Workspace", "region": "North Europe"},
        {"id": "adb-5555666677778888.7.azuredatabricks.net", "name": "EastUS-Workspace", "region": "East US"}
    ]
    
    # Compute types
    compute_types = ["All-Purpose", "Jobs", "SQL Warehouse"]
    
    # Generate project costs data
    project_costs_data = []
    
    for project in projects:
        # Base costs for each project
        base_costs = {
            "ProjectA": {"dbu": 100, "infra": 50},
            "ProjectB": {"dbu": 60, "infra": 30},
            "ProjectC": {"dbu": 30, "infra": 20},
            "ProjectD": {"dbu": 40, "infra": 25},
            "ProjectE": {"dbu": 80, "infra": 40}
        }
        
        base_dbu = base_costs[project["id"]]["dbu"]
        base_infra = base_costs[project["id"]]["infra"]
        
        current_date = start_date
        while current_date <= end_date:
            # Add some randomness to costs
            dbu_variation = np.random.normal(0, 0.2)  # 20% standard deviation
            infra_variation = np.random.normal(0, 0.15)  # 15% standard deviation
            
            # Add weekly patterns (lower costs on weekends)
            day_of_week = current_date.weekday()
            weekend_factor = 0.7 if day_of_week >= 5 else 1.0
            
            # Add monthly trend (slight increase over time)
            days_since_start = (current_date - start_date).days
            trend_factor = 1 + (days_since_start * 0.001)  # 0.1% increase per day
            
            # Calculate final costs
            dbu_cost = base_dbu * (1 + dbu_variation) * weekend_factor * trend_factor
            infra_cost = base_infra * (1 + infra_variation) * weekend_factor * trend_factor
            
            # Add some anomalies (spikes)
            if random.random() < 0.05:  # 5% chance of anomaly
                anomaly_factor = random.uniform(1.5, 3.0)
                dbu_cost *= anomaly_factor
                infra_cost *= anomaly_factor
            
            # Ensure costs are positive
            dbu_cost = max(dbu_cost, 5)
            infra_cost = max(infra_cost, 2)
            
            total_cost = dbu_cost + infra_cost
            
            # Select random compute type
            compute_type = random.choice(compute_types)
            
            # Select workspace based on region
            workspace = next((w for w in workspaces if w["region"] == project["region"]), workspaces[0])
            
            project_costs_data.append({
                "project_id": project["id"],
                "usage_date": current_date.date(),
                "workspace_id": workspace["id"],
                "region": project["region"],
                "dbu_cost": round(dbu_cost, 2),
                "infra_cost": round(infra_cost, 2),
                "total_cost": round(total_cost, 2),
                "compute_type": compute_type,
                "cluster_id": f"cluster-{uuid.uuid4().hex[:8]}",
                "environment": random.choice(["dev", "staging", "prod"]),
                "team": project["team"],
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
            
            current_date += timedelta(days=1)
    
    # Generate anomalies data
    anomalies_data = []
    
    for project in projects:
        # Find high-cost days for anomaly detection
        project_costs = [c for c in project_costs_data if c["project_id"] == project["id"]]
        
        for cost_record in project_costs:
            # 10% chance of anomaly detection
            if random.random() < 0.1:
                expected_cost = cost_record["total_cost"] * 0.8  # Assume 20% lower as expected
                anomaly_score = random.uniform(0.3, 0.9)
                
                # Determine anomaly type and severity
                if cost_record["total_cost"] > expected_cost * 1.5:
                    anomaly_type = "spike"
                    severity = random.choice(["high", "critical"])
                elif cost_record["total_cost"] < expected_cost * 0.5:
                    anomaly_type = "drop"
                    severity = random.choice(["low", "medium"])
                else:
                    anomaly_type = "trend_change"
                    severity = random.choice(["low", "medium"])
                
                anomalies_data.append({
                    "anomaly_id": str(uuid.uuid4()),
                    "project_id": project["id"],
                    "usage_date": cost_record["usage_date"],
                    "actual_cost": cost_record["total_cost"],
                    "expected_cost": round(expected_cost, 2),
                    "anomaly_score": round(anomaly_score, 4),
                    "anomaly_type": anomaly_type,
                    "severity": severity,
                    "description": f"Detected {anomaly_type} anomaly in {project['name']}",
                    "detected_at": datetime.now(),
                    "status": "open"
                })
    
    # Generate forecasts data
    forecasts_data = []
    
    for project in projects:
        # Get recent costs for forecasting
        project_costs = [c for c in project_costs_data if c["project_id"] == project["id"]]
        recent_costs = [c["total_cost"] for c in project_costs[-7:]]  # Last 7 days
        
        if recent_costs:
            avg_cost = np.mean(recent_costs)
            std_cost = np.std(recent_costs)
            
            # Generate 7-day forecast
            for i in range(1, 8):
                forecast_date = datetime.now().date() + timedelta(days=i)
                
                # Simple forecasting model
                trend_factor = 1 + (i * 0.01)  # 1% increase per day
                seasonal_factor = 1 + (0.1 * np.sin(i * 2 * np.pi / 7))  # Weekly seasonality
                noise = np.random.normal(0, 0.1)  # 10% noise
                
                predicted_cost = avg_cost * trend_factor * seasonal_factor * (1 + noise)
                confidence_range = std_cost * 0.5  # 50% of standard deviation
                
                forecasts_data.append({
                    "forecast_id": str(uuid.uuid4()),
                    "project_id": project["id"],
                    "forecast_date": forecast_date,
                    "predicted_cost": round(predicted_cost, 2),
                    "confidence_lower": round(max(predicted_cost - confidence_range, 0), 2),
                    "confidence_upper": round(predicted_cost + confidence_range, 2),
                    "model_version": "ensemble_v1.0",
                    "created_at": datetime.now()
                })
    
    # Generate pipeline logs data
    pipeline_logs_data = []
    
    pipelines = ["cost_tracking_etl", "anomaly_detection", "cost_forecasting", "pipeline_monitoring"]
    
    for pipeline in pipelines:
        # Generate logs for the last 7 days
        for i in range(7):
            log_date = datetime.now() - timedelta(days=i)
            
            # Success rate varies by pipeline
            success_rates = {
                "cost_tracking_etl": 0.98,
                "anomaly_detection": 0.95,
                "cost_forecasting": 0.92,
                "pipeline_monitoring": 0.99
            }
            
            success_rate = success_rates[pipeline]
            status = "success" if random.random() < success_rate else "failed"
            
            execution_time = random.uniform(30, 300)  # 30 seconds to 5 minutes
            rows_processed = random.randint(1000, 10000)
            
            pipeline_logs_data.append({
                "log_id": str(uuid.uuid4()),
                "pipeline_name": pipeline,
                "execution_id": str(uuid.uuid4()),
                "start_time": log_date,
                "end_time": log_date + timedelta(seconds=execution_time),
                "status": status,
                "rows_processed": rows_processed,
                "error_message": None if status == "success" else f"Error in {pipeline} execution",
                "execution_time_seconds": round(execution_time, 2),
                "created_at": datetime.now()
            })
    
    # Generate optimization recommendations
    optimization_data = []
    
    for project in projects:
        # 30% chance of having optimization recommendations
        if random.random() < 0.3:
            recommendation_types = [
                "cluster_termination",
                "instance_downgrade", 
                "storage_cleanup",
                "cost_review"
            ]
            
            recommendation_type = random.choice(recommendation_types)
            
            if recommendation_type == "cluster_termination":
                title = "Terminate Idle Cluster"
                description = f"Cluster in {project['name']} has been idle for 24+ hours"
                potential_savings = random.uniform(20, 100)
                priority = "high"
            elif recommendation_type == "instance_downgrade":
                title = "Downgrade Instance Type"
                description = f"Consider downgrading instance type in {project['name']}"
                potential_savings = random.uniform(50, 200)
                priority = "medium"
            elif recommendation_type == "storage_cleanup":
                title = "Clean Up Unused Storage"
                description = f"Remove unused data in {project['name']}"
                potential_savings = random.uniform(10, 50)
                priority = "low"
            else:
                title = "Review High Cost Project"
                description = f"Review costs for {project['name']}"
                potential_savings = random.uniform(100, 500)
                priority = "medium"
            
            optimization_data.append({
                "recommendation_id": str(uuid.uuid4()),
                "project_id": project["id"],
                "recommendation_type": recommendation_type,
                "title": title,
                "description": description,
                "potential_savings": round(potential_savings, 2),
                "priority": priority,
                "status": "pending",
                "created_at": datetime.now(),
                "implemented_at": None
            })
    
    # Generate audit logs
    audit_logs_data = []
    
    users = ["user1@company.com", "user2@company.com", "user3@company.com", "admin@company.com"]
    actions = ["dashboard_view", "data_export", "anomaly_acknowledge", "cost_review"]
    resources = ["dashboard", "table", "anomaly", "report"]
    
    for i in range(100):  # Generate 100 audit log entries
        log_time = datetime.now() - timedelta(hours=random.randint(1, 168))  # Last 7 days
        
        audit_logs_data.append({
            "audit_id": str(uuid.uuid4()),
            "user_id": random.choice(users),
            "action": random.choice(actions),
            "resource_type": random.choice(resources),
            "resource_id": random.choice([p["id"] for p in projects]),
            "ip_address": f"192.168.1.{random.randint(1, 255)}",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "accessed_at": log_time
        })
    
    return {
        "project_costs": project_costs_data,
        "anomalies": anomalies_data,
        "forecasts": forecasts_data,
        "pipeline_logs": pipeline_logs_data,
        "optimization_recommendations": optimization_data,
        "audit_logs": audit_logs_data
    }

def save_sample_data_to_csv(data, output_dir="sample_data"):
    """Save sample data to CSV files"""
    import os
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Save each dataset
    for table_name, table_data in data.items():
        if table_data:
            df = pd.DataFrame(table_data)
            filename = f"{output_dir}/{table_name}.csv"
            df.to_csv(filename, index=False)
            print(f"Saved {len(table_data)} records to {filename}")

def generate_sql_insert_statements(data):
    """Generate SQL INSERT statements for the sample data"""
    sql_statements = []
    
    for table_name, table_data in data.items():
        if table_data:
            df = pd.DataFrame(table_data)
            
            # Generate INSERT statements
            for _, row in df.iterrows():
                columns = list(row.index)
                values = []
                
                for value in row.values:
                    if pd.isna(value):
                        values.append("NULL")
                    elif isinstance(value, str):
                        values.append(f"'{value}'")
                    elif isinstance(value, datetime):
                        values.append(f"'{value.isoformat()}'")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        values.append(f"'{str(value)}'")
                
                sql = f"INSERT INTO cost_tracking.{table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                sql_statements.append(sql)
    
    return sql_statements

def main():
    """Main function to generate and save sample data"""
    print("Generating sample data for cost tracking system...")
    
    # Generate data
    sample_data = generate_sample_data()
    
    # Print summary
    print("\nSample Data Summary:")
    for table_name, table_data in sample_data.items():
        print(f"  {table_name}: {len(table_data)} records")
    
    # Save to CSV
    print("\nSaving data to CSV files...")
    save_sample_data_to_csv(sample_data)
    
    # Generate SQL statements
    print("\nGenerating SQL INSERT statements...")
    sql_statements = generate_sql_insert_statements(sample_data)
    
    # Save SQL to file
    with open("sample_data/insert_sample_data.sql", "w") as f:
        f.write("-- Sample Data INSERT Statements\n")
        f.write("-- Generated for cost tracking system testing\n\n")
        for sql in sql_statements:
            f.write(sql + "\n")
    
    print(f"Saved {len(sql_statements)} SQL INSERT statements to sample_data/insert_sample_data.sql")
    
    print("\nSample data generation completed!")
    print("\nTo load the data into Databricks:")
    print("1. Upload the CSV files to DBFS")
    print("2. Run the SQL INSERT statements")
    print("3. Or use the DataFrame API to load the data")

if __name__ == "__main__":
    main() 
