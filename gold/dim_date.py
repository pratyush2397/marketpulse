import os
import sys
import logging

os.environ["PYSPARK_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\harsh\AppData\Local\Programs\Python\Python310\python.exe"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, year, month, quarter,
    dayofweek, dayofmonth, weekofyear,
    date_format, when)

from bronze.batch_ingest import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_dim_date(spark : SparkSession, silver_path: str, dim_date_path: str):
    """Build dim_date from dates in silver. Extract date attributes for use in dashboard"""

    logger.info("Building dim_date")

    df = spark.read.format("delta").load(silver_path)

    dates_df = df.select(col("Date")).distinct()

    #Extract all date attributes

    dim_date = dates_df.select(col("Date").alias("date"),year(col("Date")).\
                               alias("year"),month(col("Date")).\
                                alias("month"),quarter(col("Date")).\
                                    alias("quarter"),dayofmonth(col("date")).\
                                        alias("day_of_month"),dayofweek(col("date")).\
                                            alias("day_of_week"),weekofyear(col("date")).\
                                                alias("week_of_year"),date_format(col("Date"), "MMMM").\
                                                    alias("month_name"),date_format(col("Date"), "EEEE").\
                                                        alias("day_name"),when(dayofweek(col("Date")).isin(1, 7), True)\
                                                            .otherwise(False).alias("is_weekend"),date_format(col("Date"), "yyyy-MM").alias("year_month"))
    
    #Add surrogate Key
    from pyspark.sql.functions import monotonically_increasing_id

    dim_date = dim_date.withColumn("date_sk",monotonically_increasing_id())

    logger.info(f"dim date rows : {dim_date.count()}")

    dim_date.show(5)

    #write to delta

    dim_date.write.format("delta").mode("overwrite").save(dim_date_path)

    logger.info("dim date written to {dim_date_path}")

    return dim_date

if __name__ =="__main__":

    spark = create_spark_session()

    build_dim_date(spark=spark,
        silver_path="D:/marketpulse/data/silver/stocks",
        dim_date_path="D:/marketpulse/data/gold/dim_date")

