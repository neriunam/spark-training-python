from pyspark.sql import SparkSession

print("START")

spark = SparkSession.builder.master("local").appName("First data frame example").getOrCreate()

orders = spark.read. \
    format("jdbc"). \
    option("url", "jdbc.mysql://ms.itversity.com"). \
    option("dbtable", "retail_db.orders"). \
    option("user", "retail_user"). \
    option("password", "itversity"). \
    load()

orders.printSchema()

print("END.")