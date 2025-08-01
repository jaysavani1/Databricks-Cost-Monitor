# Azure Databricks Cost Tracking Dashboard
![image search api](https://cleopatraenterprise.com/wp-content/uploads/2020/04/management-accounting-cost-accounting-scale.webp)
---

## 🛠️ Setup Instructions

### 1. Prerequisites
- Azure Databricks workspace
- Azure Data Factory (for orchestration)
- Unity Catalog enabled
- Python 3.9+ environment

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Create Database Schema
1. Open Databricks SQL
2. Run `sql/create_tables.sql` to create all tables
3. Run `sql/security_setup.sql` for security configuration

### 4. Upload Notebooks
1. Upload all notebooks from `notebooks/` to your Databricks workspace
2. Ensure notebooks are in the correct order (01, 02, 03, 04)

### 5. Configure Secrets
Set up Databricks secrets for:
- Azure subscription ID
- Databricks token
- Azure tenant ID

```bash
databricks secrets create-scope --scope "cost-tracking"
databricks secrets put --scope "cost-tracking" --key "azure-subscription-id" --string-value "<your-subscription-id>"
databricks secrets put --scope "cost-tracking" --key "databricks-token" --string-value "<your-token>"
databricks secrets put --scope "cost-tracking" --key "azure-tenant-id" --string-value "<your-tenant-id>"
```

## �� ADF Pipeline Setup

### Pipeline Activities
1. **01_cost_tracking_dlt.py** - ETL pipeline (run every 15 minutes)
2. **02_anomaly_detection.py** - Anomaly detection (run hourly)
3. **03_cost_forecast.py** - Cost forecasting (run every 4 hours)
4. **04_pipeline_monitoring.py** - Health monitoring (run daily)

### ADF Pipeline Configuration
```json
{
  "activities": [
    {
      "name": "CostTrackingETL",
      "type": "DatabricksNotebook",
      "dependsOn": [],
      "policy": {
        "timeout": "0.12:00:00",
        "retry": 0,
        "retryIntervalInSeconds": 30
      },
      "typeProperties": {
        "notebookPath": "/Shared/Cost Tracking/01_cost_tracking_dlt",
        "baseParameters": {}
      }
    },
    {
      "name": "AnomalyDetection",
      "type": "DatabricksNotebook",
      "dependsOn": [
        {
          "activity": "CostTrackingETL",
          "dependencyConditions": ["Succeeded"]
        }
      ]
    }
  ]
}
```

## 📊 Dashboard Setup

### 1. Create Databricks SQL Dashboard
1. Go to Databricks SQL → Dashboards
2. Create new dashboard: "Cost Tracking Dashboard"

### 2. Add Visualizations
Use queries from `sql/dashboard_queries.sql`:

- **Cost Trends**: `dashboard_cost_trends`
- **Cost Breakdown**: `dashboard_compute_breakdown`
- **Anomalies**: `dashboard_anomalies`
- **Project Summary**: `dashboard_project_summary`
- **Regional Analysis**: `dashboard_regional_analysis`
- **Optimization Recommendations**: `dashboard_optimization_recommendations`
- **Pipeline Health**: `dashboard_pipeline_health`

### 3. Chart Configuration
Use `config/dashboard_chart_config.json` for Chart.js configurations.

## 🧪 Testing

### Generate Sample Data
```bash
cd sample_data
python generate_sample_data.py
```

### Test Notebooks
1. Run notebooks in order
2. Check data in `cost_tracking.project_costs`
3. Verify anomalies in `cost_tracking.anomalies`
4. Check forecasts in `cost_tracking.forecasts`

## 📈 Key Tables

| Table | Purpose |
|-------|---------|
| `cost_tracking.project_costs` | Main cost data |
| `cost_tracking.anomalies` | Detected anomalies |
| `cost_tracking.forecasts` | Cost predictions |
| `cost_tracking.pipeline_logs` | Execution logs |
| `cost_tracking.workspace_config` | Workspace configuration |
| `cost_tracking.user_permissions` | User access permissions |
| `cost_tracking.audit_logs` | Access audit logs |
| `cost_tracking.optimization_recommendations` | Cost optimization suggestions |

## 🔍 Monitoring

### Pipeline Health
- Check `cost_tracking.pipeline_logs` for execution status
- Monitor success rates and execution times
- Set up alerts for failures

### Cost Monitoring
- Track daily costs by project
- Monitor for anomalies
- Review optimization recommendations

## 🛡️ Security

- Row-level security enabled
- Unity Catalog for data governance
- Audit logging for access tracking
- Secrets stored in Databricks secret scope

## 💰 Cost Optimization

- Uses serverless SQL compute
- Auto-termination clusters
- Delta table optimization
- Data retention (90 days)

## 🐛 Troubleshooting

### Common Issues
1. **Permission Errors**: Check Unity Catalog permissions
2. **Data Not Loading**: Verify Azure Cost Management API access
3. **Anomaly Detection Fails**: Check ML cluster configuration
4. **Forecasting Errors**: Ensure sufficient historical data

### Debug Commands
```sql
-- Check data freshness
SELECT MAX(updated_at) FROM cost_tracking.project_costs;

-- Check pipeline health
SELECT * FROM cost_tracking.pipeline_logs 
WHERE start_time >= current_date - interval '1 day';

-- Check anomalies
SELECT COUNT(*) FROM cost_tracking.anomalies 
WHERE detected_at >= current_date - interval '7 days';

-- Check forecasts
SELECT * FROM cost_tracking.forecasts 
WHERE forecast_date >= current_date;
```

## 📞 Support

- Check inline comments in notebooks
- Review SQL queries for data issues
- Monitor pipeline logs for execution problems
- Use sample data generator for testing

---

**Note**: This solution is designed to work within Azure's free tier constraints. Monitor usage to avoid unexpected charges.
