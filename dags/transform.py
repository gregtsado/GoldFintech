from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import to_date


def transform():
    spark = SparkSession.builder.appName('goldfintech').getOrCreate()
    spark_df = spark.read.csv('msftstock.csv',header= True)

    spark_df = spark_df.select(
        to_date(spark_df["week"]).alias('week'),  
        spark_df["open"].cast(FloatType()),
        spark_df["high"].cast(FloatType()),
        spark_df["low"].cast(FloatType()),
        spark_df["close"].cast(FloatType()),
        spark_df["volume"].cast(IntegerType())
    )

    spark_df.repartition(1).write.mode('overwrite').option('header','true').csv('output')
    pandas_df = spark_df.toPandas()
    spark.stop()
    pandas_df.to_csv('output/msftstock.csv', index =False)
    
