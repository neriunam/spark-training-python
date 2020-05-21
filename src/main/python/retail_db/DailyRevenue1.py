from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Revenue").set("spark.ui.port", "12920")
sc = SparkContext(conf = conf)

orders = sc.textFile("/hadoop/retail_db/orders/*")
orderItems = sc.textFile("/hadoop/retail_db/order_items/*")
ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ("COMPLETE", "CLOSED"))
ordersFilteredMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
ordersJoin = ordersFilteredMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.map(lambda o: o[1])
dailyRevenue = ordersJoinMap.reduceByKey(lambda x, y: x + y)
dailyRevenueSorted = dailyRevenue.sortByKey()
dailyRevenueSortedMap = dailyRevenueSorted.map(lambda oi: oi[0] + "," + str(oi[1]))
dailyRevenueSortedMap.saveAsTextFile("/Users/neriu/spark-warehouse/daily_revenue/file0")
