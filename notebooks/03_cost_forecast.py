"""
Cost Forecasting Model
=====================
This notebook implements time-series forecasting for Azure Databricks costs.
It uses Prophet and ARIMA models to predict project costs for the next 7 days.

Features:
- Prophet model for trend and seasonality detection
- ARIMA model for stationary time series
- Ensemble forecasting (combine multiple models)
- Confidence intervals
- Model performance evaluation
- Automated retraining
"""

import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from sklearn.metrics import mean_absolute_error, mean_squared_error
import uuid
from datetime import datetime, timedelta
import logging
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CostForecaster:
    """Time-series forecasting for project costs"""
    
    def __init__(self, forecast_days=7, confidence_level=0.95):
        self.forecast_days = forecast_days
        self.confidence_level = confidence_level
        self.models = {}
        self.model_performance = {}
        
    def prepare_data_for_prophet(self, df, project_id):
        """Prepare data for Prophet model"""
        pandas_df = df.toPandas()
        
        # Prophet requires 'ds' (date) and 'y' (value) columns
        prophet_data = pandas_df[['usage_date', 'total_cost']].copy()
        prophet_data.columns = ['ds', 'y']
        prophet_data['ds'] = pd.to_datetime(prophet_data['ds'])
        
        # Remove any missing values
        prophet_data = prophet_data.dropna()
        
        return prophet_data
    
    def fit_prophet_model(self, df, project_id):
        """Fit Prophet model for a project"""
        try:
            prophet_data = self.prepare_data_for_prophet(df, project_id)
            
            if len(prophet_data) < 7:  # Need at least 7 days of data
                logger.warning(f"Insufficient data for Prophet model: {project_id}")
                return None
            
            # Initialize Prophet model with custom parameters
            model = Prophet(
                yearly_seasonality=False,  # Disable yearly seasonality for short-term forecasting
                weekly_seasonality=True,   # Enable weekly seasonality
                daily_seasonality=False,   # Disable daily seasonality
                seasonality_mode='additive',
                changepoint_prior_scale=0.05,  # Flexibility of trend
                seasonality_prior_scale=10.0,  # Flexibility of seasonality
                interval_width=self.confidence_level
            )
            
            # Fit the model
            model.fit(prophet_data)
            
            # Store model
            self.models[f"{project_id}_prophet"] = model
            
            logger.info(f"Prophet model fitted for project: {project_id}")
            return model
            
        except Exception as e:
            logger.error(f"Error fitting Prophet model for {project_id}: {str(e)}")
            return None
    
    def fit_arima_model(self, df, project_id):
        """Fit ARIMA model for a project"""
        try:
            pandas_df = df.toPandas()
            pandas_df = pandas_df.sort_values('usage_date')
            
            if len(pandas_df) < 14:  # Need at least 14 days for ARIMA
                logger.warning(f"Insufficient data for ARIMA model: {project_id}")
                return None
            
            # Prepare time series
            ts_data = pandas_df.set_index('usage_date')['total_cost']
            
            # Check for stationarity
            adf_result = adfuller(ts_data)
            is_stationary = adf_result[1] < 0.05
            
            # Determine ARIMA parameters
            if is_stationary:
                # If stationary, use (1,0,1) or (1,0,0)
                p, d, q = 1, 0, 1
            else:
                # If not stationary, difference once
                p, d, q = 1, 1, 1
            
            # Fit ARIMA model
            model = ARIMA(ts_data, order=(p, d, q))
            fitted_model = model.fit()
            
            # Store model
            self.models[f"{project_id}_arima"] = fitted_model
            
            logger.info(f"ARIMA({p},{d},{q}) model fitted for project: {project_id}")
            return fitted_model
            
        except Exception as e:
            logger.error(f"Error fitting ARIMA model for {project_id}: {str(e)}")
            return None
    
    def forecast_prophet(self, project_id, forecast_days=None):
        """Generate forecasts using Prophet model"""
        if forecast_days is None:
            forecast_days = self.forecast_days
        
        model_key = f"{project_id}_prophet"
        if model_key not in self.models:
            logger.error(f"Prophet model not found for project: {project_id}")
            return None
        
        try:
            model = self.models[model_key]
            
            # Create future dataframe
            future = model.make_future_dataframe(periods=forecast_days)
            
            # Generate forecast
            forecast = model.predict(future)
            
            # Extract forecast results
            forecast_results = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(forecast_days)
            forecast_results.columns = ['forecast_date', 'predicted_cost', 'lower_bound', 'upper_bound']
            
            return forecast_results
            
        except Exception as e:
            logger.error(f"Error forecasting with Prophet for {project_id}: {str(e)}")
            return None
    
    def forecast_arima(self, project_id, forecast_days=None):
        """Generate forecasts using ARIMA model"""
        if forecast_days is None:
            forecast_days = self.forecast_days
        
        model_key = f"{project_id}_arima"
        if model_key not in self.models:
            logger.error(f"ARIMA model not found for project: {project_id}")
            return None
        
        try:
            model = self.models[model_key]
            
            # Generate forecast
            forecast = model.forecast(steps=forecast_days)
            
            # Create forecast dataframe
            forecast_dates = pd.date_range(
                start=datetime.now().date() + timedelta(days=1),
                periods=forecast_days,
                freq='D'
            )
            
            forecast_results = pd.DataFrame({
                'forecast_date': forecast_dates,
                'predicted_cost': forecast.values,
                'lower_bound': forecast.values * 0.8,  # Simple confidence bounds
                'upper_bound': forecast.values * 1.2
            })
            
            return forecast_results
            
        except Exception as e:
            logger.error(f"Error forecasting with ARIMA for {project_id}: {str(e)}")
            return None
    
    def ensemble_forecast(self, project_id, forecast_days=None):
        """Generate ensemble forecast combining Prophet and ARIMA"""
        if forecast_days is None:
            forecast_days = self.forecast_days
        
        prophet_forecast = self.forecast_prophet(project_id, forecast_days)
        arima_forecast = self.forecast_arima(project_id, forecast_days)
        
        if prophet_forecast is None and arima_forecast is None:
            logger.error(f"No models available for ensemble forecast: {project_id}")
            return None
        
        if prophet_forecast is None:
            return arima_forecast
        
        if arima_forecast is None:
            return prophet_forecast
        
        # Combine forecasts (simple average)
        combined_forecast = prophet_forecast.copy()
        combined_forecast['predicted_cost'] = (
            prophet_forecast['predicted_cost'] + arima_forecast['predicted_cost']
        ) / 2
        
        # Combine confidence bounds
        combined_forecast['lower_bound'] = (
            prophet_forecast['lower_bound'] + arima_forecast['lower_bound']
        ) / 2
        
        combined_forecast['upper_bound'] = (
            prophet_forecast['upper_bound'] + arima_forecast['upper_bound']
        ) / 2
        
        return combined_forecast
    
    def evaluate_model_performance(self, df, project_id):
        """Evaluate model performance using historical data"""
        try:
            # Split data into training and testing
            pandas_df = df.toPandas().sort_values('usage_date')
            split_point = int(len(pandas_df) * 0.8)
            
            train_data = pandas_df.iloc[:split_point]
            test_data = pandas_df.iloc[split_point:]
            
            if len(test_data) < 7:
                logger.warning(f"Insufficient test data for evaluation: {project_id}")
                return None
            
            # Fit models on training data
            train_df = spark.createDataFrame(train_data)
            self.fit_prophet_model(train_df, project_id)
            self.fit_arima_model(train_df, project_id)
            
            # Generate forecasts for test period
            prophet_forecast = self.forecast_prophet(project_id, len(test_data))
            arima_forecast = self.forecast_arima(project_id, len(test_data))
            ensemble_forecast = self.ensemble_forecast(project_id, len(test_data))
            
            # Calculate metrics
            actual_values = test_data['total_cost'].values
            
            performance = {}
            
            if prophet_forecast is not None:
                prophet_values = prophet_forecast['predicted_cost'].values[:len(actual_values)]
                performance['prophet'] = {
                    'mae': mean_absolute_error(actual_values, prophet_values),
                    'rmse': np.sqrt(mean_squared_error(actual_values, prophet_values)),
                    'mape': np.mean(np.abs((actual_values - prophet_values) / actual_values)) * 100
                }
            
            if arima_forecast is not None:
                arima_values = arima_forecast['predicted_cost'].values[:len(actual_values)]
                performance['arima'] = {
                    'mae': mean_absolute_error(actual_values, arima_values),
                    'rmse': np.sqrt(mean_squared_error(actual_values, arima_values)),
                    'mape': np.mean(np.abs((actual_values - arima_values) / actual_values)) * 100
                }
            
            if ensemble_forecast is not None:
                ensemble_values = ensemble_forecast['predicted_cost'].values[:len(actual_values)]
                performance['ensemble'] = {
                    'mae': mean_absolute_error(actual_values, ensemble_values),
                    'rmse': np.sqrt(mean_squared_error(actual_values, ensemble_values)),
                    'mape': np.mean(np.abs((actual_values - ensemble_values) / actual_values)) * 100
                }
            
            self.model_performance[project_id] = performance
            
            logger.info(f"Model performance evaluated for {project_id}: {performance}")
            return performance
            
        except Exception as e:
            logger.error(f"Error evaluating model performance for {project_id}: {str(e)}")
            return None

def run_cost_forecasting():
    """Run the complete cost forecasting pipeline"""
    logger.info("Starting cost forecasting pipeline")
    
    try:
        # Load historical cost data (last 60 days for training)
        historical_data = spark.sql("""
        SELECT *
        FROM cost_tracking.project_costs
        WHERE usage_date >= current_date - interval '60 days'
        ORDER BY usage_date
        """)
        
        # Get unique projects
        projects = historical_data.select("project_id").distinct().collect()
        
        all_forecasts = []
        
        for project_row in projects:
            project_id = project_row.project_id
            logger.info(f"Generating forecasts for project: {project_id}")
            
            # Filter data for this project
            project_data = historical_data.filter(col("project_id") == project_id)
            
            if project_data.count() < 14:  # Need at least 14 days of data
                logger.warning(f"Insufficient data for forecasting: {project_id}")
                continue
            
            # Initialize forecaster
            forecaster = CostForecaster(forecast_days=7)
            
            try:
                # Fit models
                forecaster.fit_prophet_model(project_data, project_id)
                forecaster.fit_arima_model(project_data, project_id)
                
                # Generate ensemble forecast
                forecast_results = forecaster.ensemble_forecast(project_id)
                
                if forecast_results is not None:
                    # Convert to records for storage
                    for _, row in forecast_results.iterrows():
                        forecast_record = {
                            "forecast_id": str(uuid.uuid4()),
                            "project_id": project_id,
                            "forecast_date": row['forecast_date'].date(),
                            "predicted_cost": float(row['predicted_cost']),
                            "confidence_lower": float(row['lower_bound']),
                            "confidence_upper": float(row['upper_bound']),
                            "model_version": "ensemble_v1.0",
                            "created_at": datetime.now()
                        }
                        all_forecasts.append(forecast_record)
                    
                    logger.info(f"Generated forecast for project {project_id}")
                
                # Evaluate model performance
                forecaster.evaluate_model_performance(project_data, project_id)
                
            except Exception as e:
                logger.error(f"Error forecasting for project {project_id}: {str(e)}")
                continue
        
        # Save forecasts to Delta table
        if all_forecasts:
            forecasts_df = spark.createDataFrame(all_forecasts)
            
            # Remove old forecasts for the same dates
            forecast_dates = [f"'{record['forecast_date']}'" for record in all_forecasts]
            date_list = ",".join(forecast_dates)
            
            spark.sql(f"""
            DELETE FROM cost_tracking.forecasts 
            WHERE forecast_date IN ({date_list})
            """)
            
            # Insert new forecasts
            forecasts_df.write.format("delta").mode("append").saveAsTable("cost_tracking.forecasts")
            
            logger.info(f"Saved {len(all_forecasts)} forecasts to cost_tracking.forecasts")
        
        # Clean up old forecasts (keep only next 14 days)
        spark.sql("""
        DELETE FROM cost_tracking.forecasts 
        WHERE forecast_date < current_date
        """)
        
        logger.info("Cost forecasting pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in cost forecasting pipeline: {str(e)}")
        raise

# Run the pipeline
if __name__ == "__main__":
    run_cost_forecasting()

# Utility functions for dashboard integration
def get_project_forecasts(project_id=None, days=7):
    """Get forecasts for dashboard display"""
    query = f"""
    SELECT *
    FROM cost_tracking.forecasts
    WHERE forecast_date >= current_date
      AND forecast_date <= current_date + interval '{days} days'
    """
    
    if project_id:
        query += f" AND project_id = '{project_id}'"
    
    query += " ORDER BY project_id, forecast_date"
    
    return spark.sql(query)

def get_forecast_accuracy():
    """Calculate forecast accuracy for recent predictions"""
    return spark.sql("""
    SELECT 
        f.project_id,
        f.forecast_date,
        f.predicted_cost,
        c.total_cost as actual_cost,
        ABS(f.predicted_cost - c.total_cost) as absolute_error,
        ABS(f.predicted_cost - c.total_cost) / c.total_cost * 100 as percentage_error
    FROM cost_tracking.forecasts f
    JOIN cost_tracking.project_costs c 
        ON f.project_id = c.project_id 
        AND f.forecast_date = c.usage_date
    WHERE f.forecast_date < current_date
      AND f.forecast_date >= current_date - interval '7 days'
    ORDER BY f.forecast_date DESC
    """)

def get_forecast_summary():
    """Get summary of current forecasts"""
    return spark.sql("""
    SELECT 
        project_id,
        COUNT(*) as forecast_count,
        AVG(predicted_cost) as avg_predicted_cost,
        MIN(predicted_cost) as min_predicted_cost,
        MAX(predicted_cost) as max_predicted_cost,
        SUM(predicted_cost) as total_predicted_cost
    FROM cost_tracking.forecasts
    WHERE forecast_date >= current_date
      AND forecast_date <= current_date + interval '7 days'
    GROUP BY project_id
    ORDER BY total_predicted_cost DESC
    """)

# Example usage for dashboard
def display_forecast_dashboard():
    """Display forecast dashboard components"""
    print("=== Cost Forecast Dashboard ===")
    
    # Current forecasts
    forecasts = get_project_forecasts(days=7)
    print(f"\nCurrent Forecasts (Next 7 days): {forecasts.count()}")
    forecasts.show()
    
    # Forecast summary
    summary = get_forecast_summary()
    print("\nForecast Summary by Project:")
    summary.show()
    
    # Forecast accuracy (if historical data available)
    accuracy = get_forecast_accuracy()
    if accuracy.count() > 0:
        print("\nRecent Forecast Accuracy:")
        accuracy.show()
        
        # Calculate overall accuracy metrics
        accuracy_df = accuracy.toPandas()
        mae = accuracy_df['absolute_error'].mean()
        mape = accuracy_df['percentage_error'].mean()
        print(f"\nOverall Accuracy Metrics:")
        print(f"Mean Absolute Error: ${mae:.2f}")
        print(f"Mean Absolute Percentage Error: {mape:.1f}%") 