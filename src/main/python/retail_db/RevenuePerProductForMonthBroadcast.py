import sys
import configparser as cp

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SQLContext, Row, functions as func

    props = cp.RawConfigParser()
    props.read("src/main/resources/application.properties")

    conf = SparkConf(). \
        setAppName("Total Revenue Per Day"). \
        setMaster(props.get(sys.argv[5], "executionMode"))

    sc = SparkContext(conf=conf)

    print("Arguments:")
    for arg in sys.argv:
        print(arg)
    """
    Arguments:
    Input base dir(1), output base dir(2), local base dir(3), month(4), environment(5)
    """
    inputPath = sys.argv[1]
    outputPath = sys.argv[2]
    localPath = sys.argv[3]
    month = sys.argv[4]

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(Configuration())

    if (fs.exists(Path(inputPath)) == False):
        print("Input path does not exists")
    else:
        if (fs.exists(Path(outputPath))):
            fs.delete(Path(outputPath), True)

        # Filter for orders which fall in the month passed as argument
        ordersCount = sc.accumulator(0)
        orders = inputPath + "/orders"


        def getOrdersTuples(rec):
            ordersCount.add(1)
            return (int(rec.split(",")[0]), 1)


        ordersFiltered = sc.textFile(orders). \
            filter(lambda order: month in order.split(",")[1]). \
            map(getOrdersTuples)

        # Join filtered orders and order_items to get order_item details for a given month
        # Get revenue for each product_id
        orderItemsCount = sc.accumulator(0)
        orderItems = inputPath + "/order_items"


        def getProductIdAndRevenue(rec):
            orderItemsCount.add(1)
            return rec[1][0]


        revenueByProductId = sc.textFile(orderItems). \
            map(lambda orderItem:
                (int(orderItem.split(",")[1]),
                 (int(orderItem.split(",")[2]), float(orderItem.split(",")[4])
                  ))
                ). \
            join(ordersFiltered). \
            map(getProductIdAndRevenue). \
            reduceByKey(lambda total, ele: total + ele)

        # We need to read products from local file system
        productsFile = open(localPath + "/products/part-00000")
        products = productsFile.read().splitlines()

        # Extract product_id and product_name and create dict of it
        # Broadcast the dict
        productsDict = dict(
            map(lambda product:
                (int(product.split(",")[0]), product.split(",")[2]), products)
        )
        bv = sc.broadcast(productsDict)

        # Get product name for each product id in revenueByProductId
        # by looking up in the broadcast variable

        revenueByProductId. \
            map(lambda product: bv.value[product[0]] + "\t" + str(product[1])). \
            saveAsTextFile(outputPath)

except ImportError as e:
    print("Can not import Spark Modules", e)
sys.exit(1)