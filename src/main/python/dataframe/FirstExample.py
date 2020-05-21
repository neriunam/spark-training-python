#import pyspark.sql as sql
from pyspark.sql import SparkSession

print("START")

spark = SparkSession.builder.master("local").appName("First data frame example").getOrCreate()

print(type(spark))
orders = spark.read.csv('/hadoop/retail_db/orders')
orders.printSchema()
orders.show()

print("END.")