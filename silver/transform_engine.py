import logging
import os
import sys

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (col, round, avg, lag, when, current_timestamp, lit)

from pyspark.sql.window import Window
from bronze.batch_ingest import create_spark_session

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

class Transform_engine:

    """
    OOP class for Silver layer transformations.
    Reads from Bronze Delta table, cleans, enriches, writes to Silver.

    Concepts covered:
    - PySpark window functions
    - Moving averages (MA7, MA30)
    - Daily return calculation
    - OOP encapsulation
    """

    def __init__(self, spark : SparkSession, bronze_path: str, silver_path : str):

        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

        logger.info(f"TransformEngine Initiliased")

    def read_bronze(self) -> DataFrame:
            """Read data from bronze layer"""
            
            logger.info(f"Reading from bronze {self.bronze_path}")
            df = self.spark.read.format("delta").load(self.bronze_path)

            logger.info(f"Read from bronze {self.bronze_path}")

            return df
    
    def clean(self, df : DataFrame) -> DataFrame:

        """Clean raw Bronze data:
        - Round price columns to 2 decimal places
        - Drop nulls
        - Remove duplicates"""
         
        logger.info("cleansing data..")
        for col_name in ["Close","High","Low","Open"]:
             df = df.withColumn(col_name, round(col(col_name),2))

        df = df.dropna(subset=['Close','High','Low','Open'])
        df = df.dropDuplicates(["Date","ticker"])

        logger.info(f"After Cleaning: {df.count()} rows")

        return df
    
    def enrich(self, df: DataFrame) -> DataFrame:
        """Add Finanacial indicator using Window Function :-
        1.  - MA7: 7-day moving average
        - MA30: 30-day moving average
        - daily_return: % change from previous day"""

        logger.info("Enriching Data with financial indicators")

        window_base = Window.partitionBy("ticker").orderBy("Date")

        #MA7 - running total 7 days
        
        window_7 = window_base.rowsBetween(-6, 0)
        df = df.withColumn("MA7", round(avg(col("Close")).over(window_7),2))

        #MA 30

        window_30 = window_base.rowsBetween(-29,0)
        df= df.withColumn("MA30", round(avg(col('Close')).over(window_30),2))

        prev_close = lag(col("close"),1).over(window_base)
        df =df.withColumn("daily_return_pct",round(((col("Close")-prev_close)/prev_close)*100,400))

        logger.info("Enrichment completed")
        
        return df
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
         
         df= df.withColumn("_ingestion_timestamp",current_timestamp()).withColumn("_layer",lit("silver")).withColumn("_source",lit("bronze"))

         return df
    
    def write_silver(self, df: DataFrame) -> DataFrame:
         
         logger.info(f"Write to silver {self.silver_path}")

         df.write.format("delta").mode("overwrite").partitionBy("ticker").save(self.silver_path)

         logger.info("Silver write completed")


    
    def run(self):
        """
        Full Silver pipeline:
        Bronze → Clean → Enrich → Silver
        """
        df = self.read_bronze()
        df = self.clean(df)
        df = self.enrich(df)
        df = self.add_audit_columns(df)
        self.write_silver(df)

        #Verify
        silver_df = self.spark.read.format("delta").load(self.silver_path)
        logger.info(f"✅ Silver rows: {silver_df.count()}")
        silver_df.select("Date", "ticker", "Close", "MA7", "MA30", "daily_return_pct").show(5)






 
    

        