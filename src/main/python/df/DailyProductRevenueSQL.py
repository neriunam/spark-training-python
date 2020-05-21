import configparser as cp, sys
from pyspark.sql import SparkSession

props = cp.RawConfigParser()
props.read('src/main/resources/application.properties')
env = sys.argv[1]

spark = SparkSession.\
    builder.\
    appName("Daily Product Revenue using Data Frames and Spark SQL").\
    master(props.get(env, 'executionMode')).\
    getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '2')

inputBaseDir = props.get(env, 'input.base.dir')
ordersCSV = spark.read. \
  csv(inputBaseDir + '/orders'). \
  toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

orderItemsCSV = spark.read. \
  csv(inputBaseDir + '/order_items'). \
  toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
       'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

from pyspark.sql.types import IntegerType, FloatType

orders = ordersCSV. \
  withColumn('order_id', ordersCSV.order_id.cast(IntegerType())). \
  withColumn('order_customer_id', ordersCSV.order_customer_id.cast(IntegerType()))

orderItems = orderItemsCSV.\
    withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

orders.createTempView('orders')
orderItems.createTempView('order_items')

spark.conf.set('spark.sql.shuffle.partitions', '2')

dailyProductRevenue = spark.sql('''select o.order_date, oi.order_item_product_id, 
             round(sum(oi.order_item_subtotal), 2) as revenue
             from orders o join order_items oi
             on o.order_id = oi.order_item_order_id
             where o.order_status in ("COMPLETE", "CLOSED")
             group by o.order_date, oi.order_item_product_id
             order by o.order_date, revenue desc''')

outputBaseDir = props.get(env, 'output.base.dir')
dailyProductRevenue.show()
#dailyProductRevenue.write.csv(outputBaseDir + '/daily_product_revenue_sql')

