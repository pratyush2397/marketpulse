import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip
import sys
import os

# Add project root to path
import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.stock_ingester import StockIngester

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """Create Local SparkSession wih delta Lake support"""
    builder = (
        SparkSession.builder.appName("MarketPulse-bronze")
        .master("local[*]")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    
    spark= configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark Session Created")

    return spark

def ingest_to_bronze(tickers : list, bronze_path: str= "D:/marketpulse/data/bronze/stocks" ):

    """
    Full Bronze ingestion pipeline:
    1. Fetch data using StockIngester (pandas)
    2. Convert to Spark DataFrame
    3. Add audit columns
    4. Write to Delta Lake Bronze table
    """

    #step1 - Fetch From Yahoo Finance

    logger.info("Step1: Fetching Stock Data")
    ingester = StockIngester(tickers=tickers,period="1Y",interval= "1d")

    pandas_df = ingester.ingest()
    logger.info(f"Fetched {len(pandas_df)} rows")

    #Step 2- Create SparkSession
    logger.info("Step 2: Creating Spark Session")
    spark = create_spark_session()

    #step 3 - Conver Pnadas Dtaframe to SparkDataframe
    logger.info(f"Starting to Convert Pandas to Spark Dataframe")
    spark_df = spark.createDataFrame(pandas_df)
    logger.info(f"Spark dataFrame created | rows :  {spark_df.count()}")

    #step 4 - Add Audit Columns (Bronze Columns)

    spark_df = (spark_df.withColumn("_ingestion_timestamp",current_timestamp())
                .withColumn("_source",lit("yahoo_finance"))
                .withColumn("_layer",lit("bronze"))
                )
    
    #step_5 - write to Delta table

    logger.info(f"Writing to delta lake at {bronze_path}...")
    (
       spark_df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("ticker")
       .save(bronze_path)
    )

    logger.info("Bronze Ingestion Successful")
    logger.info(f"Data Written to {bronze_path}")

    #step 6 - Verify

    
    verify_df = spark.read.format("delta").load(bronze_path)

    logger.info(f"✅ Verification — rows in Bronze: {verify_df.count()}")
    verify_df.show(5)

    return verify_df

if __name__ == "__main__":
    tickers = [
        "RELIANCE.NS", "TCS.NS", "INFY.NS",
        "HDFCBANK.NS", "ICICIBANK.NS"
    ]

    ingest_to_bronze(tickers=tickers)

