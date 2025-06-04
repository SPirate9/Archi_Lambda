from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaToLogs").getOrCreate()
kafka_bootstrap_servers = "localhost:9092"
input_topic = "user_logs"

logs_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .load()

logs_df = logs_stream.selectExpr("CAST(value AS STRING) as log_line")

logs_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/logs") \
    .option("checkpointLocation", "output/checkpoint/logs") \
    .trigger(processingTime="1 hour") \
    .start() \
    .awaitTermination()