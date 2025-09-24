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

    def read_weather_data(self, input_path):
        """Read weather data from CSV files with schema validation"""
        logger.info(f"Reading weather data from: {input_path}")
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
            df = self.spark.read.option("header", "true").schema(schema).csv(f"{input_path}/*.csv")
            logger.info(f"Successfully read {df.count()} records")
            return df
        except Exception as e:
            logger.error(f"Error reading data: {str(e)}")
            raise

    def clean_data(self, df):
        """Clean and validate weather data"""
        logger.info("Starting data cleaning process")
        df = df.withColumn("Date", col("Date").cast(DateType()))
        df = df.filter(col("Date").isNotNull() & col("City").isNotNull())
        df = df.withColumn("Year", year(col("Date")))
        df = df.withColumn("Month", month(col("Date")))

        # Impute missing values with the mean of the column by city
        for column in ["Temperature", "Rainfall", "Humidity", "WindSpeed", "Pressure"]:
            if column in df.columns:
                window_spec = self.spark.catalog.createExternalTable("temp_table", path=None, source=None, schema=df.schema, options={})
                mean_val = df.groupBy("City").agg(mean(col(column)).alias("mean_val"))
                df = df.join(mean_val, "City", "left")
                df = df.withColumn(column, when(col(column).isNull(), col("mean_val")).otherwise(col(column))).drop("mean_val")
        
        logger.info(f"Data cleaning finished. Record count: {df.count()}")
        return df

    def aggregate_data(self, df):
        """Aggregate weather data to get monthly averages per city"""
        logger.info("Aggregating data")
        aggregated_df = df.groupBy("Year", "Month", "City").agg(
            avg("Temperature").alias("AvgTemperature"),
            spark_sum("Rainfall").alias("TotalRainfall"),
            avg("Humidity").alias("AvgHumidity")
        ).orderBy("Year", "Month", "City")
        return aggregated_df

    def write_aggregated_data(self, df, output_path, execution_date):
        """Write the aggregated data to a Parquet file"""
        output_file = f"{output_path}/weather_summary_{execution_date}"
        logger.info(f"Writing aggregated data to: {output_file}")
        df.coalesce(1).write.mode("overwrite").parquet(output_file)
        logger.info("Data writing complete.")

def main():
    parser = argparse.ArgumentParser(description="Process weather data.")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("WeatherDataProcessing").getOrCreate()
    processor = WeatherDataProcessor(spark)

    try:
        raw_df = processor.read_weather_data(args.input_path)
        cleaned_df = processor.clean_data(raw_df)
        aggregated_df = processor.aggregate_data(cleaned_df)
        processor.write_aggregated_data(aggregated_df, args.output_path, args.execution_date)
        logger.info("Weather data processing completed successfully")
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()