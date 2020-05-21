orderspath = "/hadoop/retail_db/orders/part-00000"
orderitemspath = "/hadoop/retail_db/order_items/part-00000"

ordersdata = open(orderspath).read()
orderitemsdata = open(orderitemspath).read()

print(type(ordersdata))
print(type(orderitemsdata))

orderslist = ordersdata.splitlines()
orderitemslist = orderitemsdata.splitlines()

print(len(orderslist))
print(len(orderitemslist))

print("First 5 orders")
for item in orderslist[:5]:
    print(item)

print("\nFirst 5 orders items")
for item in orderitemslist[:5]:
    print(item)
