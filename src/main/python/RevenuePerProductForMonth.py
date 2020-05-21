import sys
import configparser as cp

try:
    from pyspark import SparkConf, SparkContext
    print("Arguments:")
    for arg in sys.argv:
        print(arg)

    props = cp.RawConfigParser()
    props.read("src/main/resources/application.properties")
    conf = SparkConf().\
        setAppName("Total Revenue Per Day").\
        setMaster(props.get(sys.argv[5], "executionMode"))
    sc = SparkContext(conf = conf)

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
        print("Input path: '" + inputPath + "' doesn't exists")
    else:
        if (fs.exists(Path(outputPath))):
            fs.delete(Path(outputPath), True)

        orders = inputPath + "/orders"
        ordersFiltered = sc.textFile(orders).\
            filter(lambda order: order.split(",")[1].startswith(month)). \
            map(lambda order: (int(order.split(",")[0]), 1))
        # (order_id, 1)

        orderItems = inputPath + "/order_items"
        revenueByProductId = sc.textFile(orderItems). \
            map(lambda orderItem:
                (int(orderItem.split(",")[1]),
                 (int(orderItem.split(",")[2]), float(orderItem.split(",")[4])
                ))
            ). \
            join(ordersFiltered). \
            map(lambda rec: rec[1][0]). \
            reduceByKey(lambda total, ele: total + ele)
        # after first map -> (order_item_order_id, (order_item_product_id, order_item_subtotal)
        # after join ->  (order_id, ((order_item_product_id, order_item_subtotal), 1))
        # after second map -> (order_item_product_id, order_item_subtotal)

        productFile = open(localPath + "/products/part-00000")
        products = productFile.read().splitlines()
        # After map -> (product_id, product_name)
        # After join -> (product_id, (product_name, product_revenue))
        sc.parallelize(products). \
            map(lambda product:
                        (int(product.split(",")[0]), product.split(",")[2])
                ). \
            join(revenueByProductId). \
            map(lambda product: product[1][0] + "\t" + str(product[1][1])). \
            saveAsTextFile(outputPath)

except ImportError as e:
    print("Couldn't import spark modules", e)

sys.exit(1)
