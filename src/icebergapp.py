from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder \
    .appName("IcebergApp") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "output/iceberg/warehouse") \
    .getOrCreate()

df = spark.read.option("header", "false").csv("output/logs/*.csv") \
    .toDF("log_line")

df = df.withColumn("timestamp", split(col("log_line"), ",")[0]) \
       .withColumn("ip", split(col("log_line"), ",")[1]) \
       .withColumn("user_agent", split(col("log_line"), ",")[2])

df.writeTo("hadoop_cat.logs_iceberg").using("iceberg").createOrReplace()
df_iceberg = spark.read.format("iceberg").load("output/iceberg/warehouse/logs_iceberg")
df_iceberg.show()