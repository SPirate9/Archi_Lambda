from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, to_date

def main():
    spark = SparkSession.builder.appName("BatchLayer").getOrCreate()

    logs_df = spark.read.option("header", "false").csv("output/logs/*.csv") \
        .toDF("log_line")

    logs_df = logs_df.withColumn("timestamp", split(col("log_line"), ",")[0]) \
                     .withColumn("ip", split(col("log_line"), ",")[1]) \
                     .withColumn("user_agent", split(col("log_line"), ",")[2]) \
                     .withColumn("timestamp", col("timestamp").cast("timestamp"))

    connections_per_ip = logs_df.groupBy("ip").agg(count("*").alias("connection_count"))
    connections_per_ip.write.mode("overwrite").csv("output/batch/connections_per_ip")

    connections_per_agent = logs_df.groupBy("user_agent").agg(count("*").alias("connection_count"))
    connections_per_agent.write.mode("overwrite").csv("output/batch/connections_per_agent")

    connections_per_day = logs_df.withColumn("date", to_date(col("timestamp"))) \
        .groupBy("date").agg(count("*").alias("connection_count"))
    connections_per_day.write.mode("overwrite").csv("output/batch/connections_per_day")

    spark.stop()

if __name__ == "__main__":
    main()