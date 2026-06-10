import os
import sys
import logging
from datetime import date

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from bronze.batch_ingest import create_spark_session
from pyspark.sql.functions import current_date, to_date, lit
import yfinance as yf

logging.basicConfig(level= logging.INFO)
logger = logging.getLogger(__name__)

def get_stock_metadata(tickers: list) -> list:
    """Fetch company metadata from Yahoo Finance.
    Returns list of dicts with stock attributes."""

    records = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            records.append({"ticker":       ticker,
                "company_name": info.get("longName", "Unknown"),
                "sector":       info.get("sector", "Unknown"),
                "industry":     info.get("industry", "Unknown"),
                "exchange":     info.get("exchange", "Unknown"),
                "market_cap":   info.get("marketCap", 0),})
            
            logger.info(f"✅ Metadata fetched for {ticker}")

        except Exception as e:
            logger.error(f"Failed for {ticker}: {e}")

    return records

def build_dim_stock(spark : SparkSession, tickers: list, dim_stock_path: str):
    """
    Build dim_stock with SCD Type 2 using Delta MERGE.

    SCD Type 2 logic:
    - New ticker → INSERT with valid_from=today, valid_to=9999-12-31, is_current=True
    - Changed sector/industry → expire old record, INSERT new record
    - No change → do nothing
    """

    logger.info("Building Dimension Stock")

    #fetch current metadata

    metadata = get_stock_metadata(tickers)
    source_df = spark.createDataFrame(metadata)

    # Add SCD 2  columns to incoming data

    source_df = source_df.withColumn("valid_from", F.current_date()).\
        withColumn("valid_to",F.to_date(lit("9999-12-31"))).\
            withColumn( "is_current",F.lit(True))
    
    source_df.show(5)

    #check if dim stock already exist
    import os

    if not os.path.exists(dim_stock_path.replace("/","\\")):
        logger.info("Fisrt Load - writitng dim stock directly")

        from pyspark.sql.functions import monotonically_increasing_id
        source_df = source_df.withColumn("stock_sk", monotonically_increasing_id())

        source_df.write.format("delta").mode("overwrite").save(dim_stock_path)

        logger.info("{source_df.count()} rowsData Successfully written to {dim_stock_path}")
        return
    
    #SCD merge for subsequent loads

    logger.info("Subsequent load — applying SCD Type 2 MERGE")

    delta_table = DeltaTable.forPath(spark,dim_stock_path)

    #Expire change record

    delta_table.\
        alias("target").merge(source_df.\
                              alias("source"), "target.ticker = source.ticker AND target.is_current = true").\
                                        whenMatchedUpdate(condition = "target.sector!=source.sector OR target.industry != source.industry",
                                                          set = {
                                                              "valid_to": "current_date()",
                                                              "is_current": "false"
                                                          }).execute()
    
    # Insert new change record

    source_df.write.format("delta").mode("overwrite").save(dim_stock_path)

     # Verify
    result = spark.read.format("delta").load(dim_stock_path)
    logger.info(f"✅ dim_stock rows: {result.count()}")
    result.show()

if __name__ == "__main__":
    spark = create_spark_session()

    tickers = [
        "RELIANCE.NS", "TCS.NS", "INFY.NS",
        "HDFCBANK.NS", "ICICIBANK.NS"
    ]

    build_dim_stock(
        spark=spark,
        tickers=tickers,
        dim_stock_path="D:/marketpulse/data/gold/dim_stock"
    )


            
