from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import *
import configparser as cp
import sys

props = cp.RawConfigParser()
props.read('src/main/resources/application.properties')
env = sys.argv[1]

print('Execution environment: ' + env)
print('Properties:')
for i in props.items(env):
    print(i)

spark = SparkSession. \
    builder. \
    master(props.get(env, 'executionMode')). \
    appName("First data frame example"). \
    getOrCreate()

inputBaseDir = props.get(env, 'input.base.dir')
outputBaseDir = props.get(env, 'output.base.dir')

ordersCSV = spark. \
    read. \
    csv(inputBaseDir +  '/orders'). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")

orderItemsCSV = spark. \
    read. \
    csv(inputBaseDir + "/order_items"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id",
         "order_item_quantity", "order_item_subtotal", "order_item_product_price")

orders = ordersCSV. \
    withColumn("order_id", ordersCSV.order_id.cast(IntegerType())). \
    withColumn("order_customer_id", ordersCSV.order_customer_id.cast(IntegerType()))

# orders.printSchema()
# orders.show()

orderItems = orderItemsCSV. \
    withColumn("order_item_id", orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn("order_item_order_id", orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn("order_item_product_id", orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn("order_item_quantity", orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn("order_item_subtotal", orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn("order_item_product_price", orderItemsCSV.order_item_product_price.cast(FloatType()))

# orderItems.printSchema()
# orderItems.show()

spark.conf.set('spark.sql.shuffle.partitions', '2')

dailyProductRevenue = orders.where('order_status in ("COMPLETE", "CLOSED")'). \
join(orderItems, orders.order_id == orderItems.order_item_order_id). \
groupBy('order_date', 'order_item_product_id'). \
agg(round(sum('order_item_subtotal'), 2).alias('revenue'))

dailyProductRevenueSorted = dailyProductRevenue.orderBy('order_date', dailyProductRevenue.revenue.desc())
dailyProductRevenueSorted.show()

#dailyProductRevenueSorted.write.csv(outputBaseDir + '/daily_product_revenue')

print("End")
