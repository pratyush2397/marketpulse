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

def build_aggregations(spark: SparkSession, fact_path: str, agg_path: str):
    """
    Build aggregate tables from fact_trades.
    
    Aggregations:
    1. agg_daily    — avg close, total volume per ticker per day
    2. agg_sector   — avg return, avg close per sector per month
    3. agg_top_stocks — top 5 stocks by avg daily return
    """
    logger.info("Building aggregations...")

    fact_df = spark.read.format("delta").load(fact_path)
    logger.info(f"fact_trades rows: {fact_df.count()}")

    # 1. Daily aggregation per ticker
    agg_daily = fact_df.groupBy("trade_date", "ticker", "sector") \
        .agg(
            F.round(F.avg("Close"), 2).alias("avg_close"),
            F.round(F.avg("MA7"), 2).alias("avg_ma7"),
            F.round(F.avg("MA30"), 2).alias("avg_ma30"),
            F.sum("Volume").alias("total_volume"),
            F.round(F.avg("daily_return_pct"), 4).alias("avg_daily_return")
        )

    logger.info(f"agg_daily rows: {agg_daily.count()}")
    agg_daily.show(5)

    agg_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{agg_path}/agg_daily")

    # 2. Monthly sector aggregation
    agg_sector = fact_df.groupBy(
        F.date_format("trade_date", "yyyy-MM").alias("year_month"),
        "sector"
    ).agg(
        F.round(F.avg("Close"), 2).alias("avg_close"),
        F.round(F.avg("daily_return_pct"), 4).alias("avg_return"),
        F.sum("Volume").alias("total_volume"),
        F.countDistinct("ticker").alias("num_stocks")
    )

    logger.info(f"agg_sector rows: {agg_sector.count()}")
    agg_sector.show(5)

    agg_sector.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{agg_path}/agg_sector")

    # 3. Top stocks by average daily return
    agg_top_stocks = fact_df.groupBy("ticker", "sector") \
        .agg(
            F.round(F.avg("daily_return_pct"), 4).alias("avg_daily_return"),
            F.round(F.avg("Close"), 2).alias("avg_close"),
            F.sum("Volume").alias("total_volume")
        ) \
        .orderBy(F.desc("avg_daily_return")) \
        .limit(10)

    logger.info("Top 10 stocks by avg daily return:")
    agg_top_stocks.show()

    agg_top_stocks.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{agg_path}/agg_top_stocks")

    logger.info(f"✅ All aggregations written to {agg_path}")


if __name__ == "__main__":
    spark = create_spark_session()

    build_aggregations(
        spark=spark,
        fact_path="D:/marketpulse/data/gold/fact_trades",
        agg_path="D:/marketpulse/data/gold/aggregations"
    )