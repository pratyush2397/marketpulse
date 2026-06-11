import os
import sys
import logging

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from bronze.batch_ingest import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_fact_trades(spark : SparkSession, silver_path : str, 
                      dim_date_path : str, dim_stock_path : str, fact_path : str):
    """Build fact_trades by joining silver data with dimension table. One row per ticker per day"""

    logger.info("Building fact trades")

    #read silver

    silver_df = spark.read.format("delta").load(silver_path)
    logger.info(f"silver rows {silver_df}.count()")

    #read dimension

    dim_date_df = spark.read.format("delta").load(dim_date_path)
    dim_stock_df = spark.read.format("delta").\
        load(dim_stock_path).filter(F.col("is_current")==True)
    
    fact_df = silver_df.join(
    dim_date_df.select("date", "date_sk"),
    silver_df["Date"] == dim_date_df["date"],
    "left").drop(dim_date_df["date"]) 

    fact_df = fact_df.join(
        dim_stock_df.select("ticker", "stock_sk", "sector"),
        on="ticker",
        how="left"
    )

    fact_df = fact_df.select(
        F.monotonically_increasing_id().alias("trade_id"),
        F.col("date_sk"),
        F.col("stock_sk"),
        F.col("ticker"),
        F.col("sector"),
        F.col("Date").alias("trade_date"),
        F.col("Open"),
        F.col("High"),
        F.col("Low"),
        F.col("Close"),
        F.col("Volume"),
        F.col("MA7"),
        F.col("MA30"),
        F.col("daily_return_pct"),
        F.current_timestamp().alias("_load_timestamp"),
        F.lit("silver").alias("_source"),
        F.lit("gold").alias("_layer")
    )

    logger.info(f"fact_trades rows: {fact_df.count()}")
    fact_df.show(5)

    fact_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ticker") \
        .save(fact_path)
    
    logger.info(f"fact_trades written to {fact_path}")


if __name__ == "__main__":
    spark = create_spark_session()

    build_fact_trades(
        spark=spark,
        silver_path="D:/marketpulse/data/silver/stocks",
        dim_date_path="D:/marketpulse/data/gold/dim_date",
        dim_stock_path="D:/marketpulse/data/gold/dim_stock",
        fact_path="D:/marketpulse/data/gold/fact_trades"
    )