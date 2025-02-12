from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

order_schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType()),
            StructField("customer_id", StringType()),
            StructField("total_amount", StringType()),
            StructField("status", StringType())
        ]))
    ]))
])

spark = SparkSession \
    .builder \
    .appName("OrdersStreaming") \
    .getOrCreate()

orders_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce.public.orders") \
    .load()

parsed_orders = orders_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select(
        "data.payload.after.status",
        expr("CAST(data.payload.after.total_amount AS DOUBLE)").alias("total_amount")
    )

metrics = parsed_orders \
    .groupBy("status") \
    .agg({
        "status": "count",
        "total_amount": "sum"
    }) \
    .select(
        col("status"),
        col("count(status)").alias("order_count"),
        col("sum(total_amount)").alias("total_sales")
    )

query = metrics \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()