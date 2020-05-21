from pyspark import SparkConf, SparkContext

import configparser as cp
import sys

props = cp.RawConfigParser()
props.read("src/main/resources/application.properties")
env = sys.argv[1]

conf = SparkConf().\
    setMaster(props.get(env, 'executionMode')).\
    setAppName("Revenue").\
    set("spark.ui.port", "12920")
sc = SparkContext(conf = conf)

orders = sc.textFile(props.get(env, 'input.base.dir') + "/orders/*")
orderItems = sc.textFile(props.get(env, 'input.base.dir') + "/order_items/*")
ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ("COMPLETE", "CLOSED"))
ordersFilteredMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
ordersJoin = ordersFilteredMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.map(lambda o: o[1])
dailyRevenue = ordersJoinMap.reduceByKey(lambda x, y: x + y)
dailyRevenueSorted = dailyRevenue.sortByKey()
dailyRevenueSortedMap = dailyRevenueSorted.map(lambda oi: oi[0] + "," + str(oi[1]))

for i in dailyRevenueSortedMap.take(10): print(i)
dailyRevenueSortedMap.saveAsTextFile(props.get(env, 'output.base.dir') + "/dailyrevenue1")