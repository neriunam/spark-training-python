#import pyspark.sql as sql
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

print("START")

spark = SparkSession.builder.master("local").appName("First data frame example").getOrCreate()

ordersCSV = spark. \
    read. \
    csv('/hadoop/retail_db/orders'). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")

orderItemsCSV = spark. \
    read. \
    csv("/hadoop/retail_db/order_items"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id",
         "order_item_quantity", "order_item_subtotal", "order_item_product_price")

orders = ordersCSV. \
    withColumn("order_id", ordersCSV.order_id.cast(IntegerType())). \
    withColumn("order_customer_id", ordersCSV.order_customer_id.cast(IntegerType()))

orderItems = orderItemsCSV. \
    withColumn("order_item_id", orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn("order_item_order_id", orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn("order_item_product_id", orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn("order_item_quantity", orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn("order_item_subtotal", orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn("order_item_product_price", orderItemsCSV.order_item_product_price.cast(FloatType()))


orders.printSchema()
orderItems.printSchema()

print("END.")