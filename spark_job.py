import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, mean, stddev, percentile_approx,
    year, month, dayofmonth, avg, sum as spark_sum, max as spark_max, 
    min as spark_min, count, round as spark_round, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherDataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def create_spark_session(self, app_name="WeatherDataProcessing"):
        """Create Spark session with optimized configuration"""
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        # Set log level to reduce verbose output
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def read_weather_data(self, input_path):
        """Read weather data from CSV files with schema validation"""
        logger.info(f"Reading weather data from: {input_path}")
        
        # Define schema for better performance and type safety
        schema = StructType([
            StructField("Date", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Temperature", DoubleType(), True),
            StructField("Rainfall", DoubleType(), True),
            StructField("Humidity", DoubleType(), True),
            StructField("WindSpeed", DoubleType(), True),
            StructField("Pressure", DoubleType(), True)
        ])
        
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(schema) \
                .csv(f"{input_path}/*.csv")
            
            logger.info(f"Successfully read {df.count()} records")
            return df
            
        except Exception as e:
            logger.error(f"Error reading data: {str(e)}")
            raise
    
    def clean_data(self, df):
        """Clean and validate weather data"""
        logger.info("Starting data cleaning process")
        
        initial_count = df.count()
        logger.info(f"Initial record count: {initial_count}")
        
        # Convert Date column to proper date type
        df = df.withColumn("Date", col("Date").cast(DateType()))
        
        # Remove records with null dates or cities (critical fields)
        df = df.filter(col("Date").isNotNull() & col("City").isNotNull())
        
        # Add year, month columns for partitioning and aggregation
        df = df.withColumn("Year", year(col("Date"))) \