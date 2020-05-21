ordersPath = "/hadoop/retail_db/orders/part-00000"
orderItemsPath = "/hadoop/retail_db/order_items/part-00000"

def readData(dataPath):
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList

def printList(list):
    print("\nPrint list, length: " + str(len(list)))
    for item in list:
        print(item)

orders = readData(ordersPath)


# Orders with status "COMPLETE"
ordersFiltered = []
for order in orders:
    if (order.split(",")[3] == "COMPLETE"):
        ordersFiltered.append(order)
printList(ordersFiltered[:10])

ordersCompleted = filter(lambda order: order.split(",")[3] == "COMPLETE", orders)
printList(ordersCompleted[:10])


# Get orderId and order status
ordersMap = []
for order in orders:
    ordersMap.append((int(order.split(",")[0]), order.split(",")[3]))

printList(ordersMap[:10])

# Get orderi and subtotal
orderItems = readData(orderItemsPath)
orderItemsMap = []
for orderItem in orderItems:
    orderItemsMap.append((int(orderItem.split(",")[1]), float(orderItem.split(",")[4])))

printList(orderItemsMap[:10])


# order month
ordersMap = []
for order in orders:
    ordersMap.append(order.split(",")[1][:7])

printList(ordersMap[:10])