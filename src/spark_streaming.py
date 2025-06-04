from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, col

spark = SparkSession.builder.appName("SpeedLayer").getOrCreate()
kafka_bootstrap_servers = "localhost:9092"
input_topic = "user_logs"

logs_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .load()

logs_df = logs_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr("split(value, ',')[0] as timestamp", 
                "split(value, ',')[1] as ip", 
                "split(value, ',')[2] as user_agent") \
    .selectExpr("to_timestamp(timestamp) as timestamp", "ip", "user_agent") \
    .withWatermark("timestamp", "1 minute")

connections_per_ip = logs_df.groupBy("ip", "timestamp").agg(count("ip").alias("connection_count"))
connections_per_agent = logs_df.groupBy("user_agent", "timestamp").agg(count("user_agent").alias("connection_count"))

connections_per_day = logs_df.groupBy(window("timestamp", "1 day")).agg(count("ip").alias("connection_count")) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

connections_per_ip.writeStream.outputMode("append").format("csv") \
    .option("path", "output/connections_per_ip") \
    .option("checkpointLocation", "output/checkpoint/ip").start()

connections_per_agent.writeStream.outputMode("append").format("csv") \
    .option("path", "output/connections_per_agent") \
    .option("checkpointLocation", "output/checkpoint/agent").start()

connections_per_day.writeStream.outputMode("append").format("csv") \
    .option("path", "output/connections_per_day") \
    .option("checkpointLocation", "output/checkpoint/day").start()

spark.streams.awaitAnyTermination()