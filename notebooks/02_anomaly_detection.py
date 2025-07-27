"""
Cost Anomaly Detection Model
===========================
This notebook implements machine learning-based anomaly detection for Azure Databricks costs.
It uses Isolation Forest and statistical thresholding to identify unusual cost patterns.

Features:
- Isolation Forest for unsupervised anomaly detection
- Statistical thresholding (z-score, IQR)
- Multi-dimensional analysis (cost, usage patterns, trends)
- Configurable sensitivity levels
- Automated alert generation
"""

import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
import uuid
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CostAnomalyDetector:
    """Cost anomaly detection using multiple algorithms"""
    
    def __init__(self, contamination=0.1, random_state=42):
        self.contamination = contamination
        self.random_state = random_state
        self.isolation_forest = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100
        )
        self.scaler = SklearnStandardScaler()
        self.is_fitted = False
        
    def prepare_features(self, df):
        """Prepare features for anomaly detection"""
        # Convert to pandas for sklearn
        pandas_df = df.toPandas()
        
        # Create features
        features = []
        
        # Cost-based features
        features.append(pandas_df['total_cost'].values.reshape(-1, 1))
        features.append(pandas_df['dbu_cost'].values.reshape(-1, 1))
        features.append(pandas_df['infra_cost'].values.reshape(-1, 1))
        
        # Ratio features
        cost_ratio = pandas_df['dbu_cost'] / (pandas_df['total_cost'] + 1e-8)
        features.append(cost_ratio.values.reshape(-1, 1))
        
        # Day of week (cyclical encoding)
        pandas_df['usage_date'] = pd.to_datetime(pandas_df['usage_date'])
        day_of_week = pandas_df['usage_date'].dt.dayofweek
        day_sin = np.sin(2 * np.pi * day_of_week / 7)
        day_cos = np.cos(2 * np.pi * day_of_week / 7)
        features.append(day_sin.values.reshape(-1, 1))
        features.append(day_cos.values.reshape(-1, 1))
        
        # Combine features
        X = np.hstack(features)
        
        return X, pandas_df
    
    def fit(self, df):
        """Fit the anomaly detection model"""
        X, _ = self.prepare_features(df)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Fit isolation forest
        self.isolation_forest.fit(X_scaled)
        self.is_fitted = True
        
        logger.info("Anomaly detection model fitted successfully")
        
    def predict(self, df):
        """Predict anomalies in the data"""
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X, pandas_df = self.prepare_features(df)
        X_scaled = self.scaler.transform(X)
        
        # Predict anomalies (-1 for anomaly, 1 for normal)
        predictions = self.isolation_forest.predict(X_scaled)
        
        # Get anomaly scores
        scores = self.isolation_forest.decision_function(X_scaled)
        
        # Add predictions to dataframe
        pandas_df['anomaly_prediction'] = predictions
        pandas_df['anomaly_score'] = scores
        
        return pandas_df
    
    def detect_statistical_anomalies(self, df, threshold=2.0):
        """Detect anomalies using statistical methods (z-score)"""
        pandas_df = df.toPandas()
        
        # Calculate z-scores for different metrics
        for metric in ['total_cost', 'dbu_cost', 'infra_cost']:
            mean_val = pandas_df[metric].mean()
            std_val = pandas_df[metric].std()
            z_scores = np.abs((pandas_df[metric] - mean_val) / (std_val + 1e-8))
            
            pandas_df[f'{metric}_zscore'] = z_scores
            pandas_df[f'{metric}_anomaly'] = z_scores > threshold
        
        return pandas_df

def calculate_expected_costs(df):
    """Calculate expected costs using moving averages"""
    pandas_df = df.toPandas()
    
    # Sort by date
    pandas_df = pandas_df.sort_values('usage_date')
    
    # Calculate moving averages
    pandas_df['expected_cost_7d'] = pandas_df['total_cost'].rolling(window=7, min_periods=1).mean()
    pandas_df['expected_cost_30d'] = pandas_df['total_cost'].rolling(window=30, min_periods=1).mean()
    
    # Calculate trend
    pandas_df['cost_trend'] = pandas_df['total_cost'].rolling(window=7, min_periods=2).apply(
        lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0
    )
    
    return pandas_df

def generate_anomaly_records(anomalies_df, project_id):
    """Generate anomaly records for storage"""
    anomaly_records = []
    
    for _, row in anomalies_df.iterrows():
        # Determine anomaly type and severity
        anomaly_type = "unknown"
        severity = "low"
        description = ""
        
        # Check for cost spikes
        if row.get('total_cost_anomaly', False):
            anomaly_type = "spike"
            cost_increase = ((row['total_cost'] - row['expected_cost_7d']) / row['expected_cost_7d']) * 100
            description = f"Cost spike: ${row['total_cost']:.2f} (expected: ${row['expected_cost_7d']:.2f}, +{cost_increase:.1f}%)"
            
            if cost_increase > 100:
                severity = "critical"
            elif cost_increase > 50:
                severity = "high"
            elif cost_increase > 25:
                severity = "medium"
        
        # Check for isolation forest anomalies
        elif row.get('anomaly_prediction', 1) == -1:
            anomaly_type = "pattern_anomaly"
            description = f"Unusual cost pattern detected (score: {row['anomaly_score']:.3f})"
            
            if abs(row['anomaly_score']) > 0.5:
                severity = "high"
            elif abs(row['anomaly_score']) > 0.3:
                severity = "medium"
        
        # Check for trend changes
        elif abs(row.get('cost_trend', 0)) > 10:
            anomaly_type = "trend_change"
            description = f"Cost trend change detected (slope: {row['cost_trend']:.2f})"
            severity = "medium"
        
        # Only create records for actual anomalies
        if anomaly_type != "unknown":
            anomaly_records.append({
                "anomaly_id": str(uuid.uuid4()),
                "project_id": project_id,
                "usage_date": row['usage_date'],
                "actual_cost": float(row['total_cost']),
                "expected_cost": float(row['expected_cost_7d']),
                "anomaly_score": float(row.get('anomaly_score', 0)),
                "anomaly_type": anomaly_type,
                "severity": severity,
                "description": description,
                "detected_at": datetime.now(),
                "status": "open"
            })
    
    return anomaly_records

# Main anomaly detection pipeline
def run_anomaly_detection():
    """Run the complete anomaly detection pipeline"""
    logger.info("Starting anomaly detection pipeline")
    
    try:
        # Load recent cost data (last 30 days for training, last 7 days for detection)
        training_data = spark.sql("""
        SELECT *
        FROM cost_tracking.project_costs
        WHERE usage_date >= current_date - interval '30 days'
          AND usage_date < current_date - interval '7 days'
        ORDER BY usage_date
        """)
        
        detection_data = spark.sql("""
        SELECT *
        FROM cost_tracking.project_costs
        WHERE usage_date >= current_date - interval '7 days'
        ORDER BY usage_date
        """)
        
        # Get unique projects
        projects = detection_data.select("project_id").distinct().collect()
        
        all_anomalies = []
        
        for project_row in projects:
            project_id = project_row.project_id
            logger.info(f"Processing anomalies for project: {project_id}")
            
            # Filter data for this project
            project_training = training_data.filter(col("project_id") == project_id)
            project_detection = detection_data.filter(col("project_id") == project_id)
            
            if project_training.count() == 0 or project_detection.count() == 0:
                logger.warning(f"Insufficient data for project {project_id}")
                continue
            
            # Initialize and fit anomaly detector
            detector = CostAnomalyDetector(contamination=0.1)
            
            try:
                # Fit model on training data
                detector.fit(project_training)
                
                # Detect anomalies in recent data
                anomalies_df = detector.predict(project_detection)
                
                # Add statistical anomaly detection
                anomalies_df = detector.detect_statistical_anomalies(project_detection)
                
                # Calculate expected costs
                anomalies_df = calculate_expected_costs(anomalies_df)
                
                # Generate anomaly records
                project_anomalies = generate_anomaly_records(anomalies_df, project_id)
                all_anomalies.extend(project_anomalies)
                
                logger.info(f"Found {len(project_anomalies)} anomalies for project {project_id}")
                
            except Exception as e:
                logger.error(f"Error processing project {project_id}: {str(e)}")
                continue
        
        # Save anomalies to Delta table
        if all_anomalies:
            anomalies_df = spark.createDataFrame(all_anomalies)
            anomalies_df.write.format("delta").mode("append").saveAsTable("cost_tracking.anomalies")
            
            logger.info(f"Saved {len(all_anomalies)} anomalies to cost_tracking.anomalies")
            
            # Generate alerts for high-severity anomalies
            high_severity = anomalies_df.filter(col("severity").isin(["high", "critical"]))
            if high_severity.count() > 0:
                logger.warning(f"Found {high_severity.count()} high-severity anomalies requiring attention")
                
                # Here you would integrate with your alerting system
                # For example, send email notifications or create Databricks alerts
                
        else:
            logger.info("No anomalies detected")
        
        # Clean up old anomalies (keep only last 30 days)
        spark.sql("""
        DELETE FROM cost_tracking.anomalies 
        WHERE usage_date < current_date - interval '30 days'
        """)
        
        logger.info("Anomaly detection pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in anomaly detection pipeline: {str(e)}")
        raise

# Run the pipeline
if __name__ == "__main__":
    run_anomaly_detection()

# Additional utility functions for dashboard integration
def get_recent_anomalies(project_id=None, severity=None, days=7):
    """Get recent anomalies for dashboard display"""
    query = f"""
    SELECT *
    FROM cost_tracking.anomalies
    WHERE usage_date >= current_date - interval '{days} days'
    """
    
    if project_id:
        query += f" AND project_id = '{project_id}'"
    
    if severity:
        query += f" AND severity = '{severity}'"
    
    query += " ORDER BY detected_at DESC"
    
    return spark.sql(query)

def get_anomaly_summary():
    """Get summary statistics of anomalies"""
    return spark.sql("""
    SELECT 
        severity,
        anomaly_type,
        COUNT(*) as count,
        AVG(actual_cost) as avg_actual_cost,
        AVG(expected_cost) as avg_expected_cost,
        AVG(anomaly_score) as avg_anomaly_score
    FROM cost_tracking.anomalies
    WHERE usage_date >= current_date - interval '30 days'
    GROUP BY severity, anomaly_type
    ORDER BY severity, count DESC
    """)

# Example usage for dashboard
def display_anomaly_dashboard():
    """Display anomaly dashboard components"""
    print("=== Cost Anomaly Dashboard ===")
    
    # Recent anomalies
    recent = get_recent_anomalies(days=7)
    print(f"\nRecent Anomalies (Last 7 days): {recent.count()}")
    recent.show()
    
    # Summary by severity
    summary = get_anomaly_summary()
    print("\nAnomaly Summary by Severity:")
    summary.show()
    
    # High-severity anomalies requiring attention
    high_severity = get_recent_anomalies(severity="high", days=7)
    if high_severity.count() > 0:
        print(f"\n⚠️  High-Severity Anomalies Requiring Attention: {high_severity.count()}")
        high_severity.select("project_id", "usage_date", "actual_cost", "description").show() 