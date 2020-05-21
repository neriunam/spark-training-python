import pandas as pd

orderItemsPath = "/hadoop/retail_db/order_items/part-00000"
orderItems = pd.read_csv(orderItemsPath, names=["order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price"])

print("\n---------------")
#print(orderItems[['order_item_id', 'order_item_order_id', 'order_item_subtotal']])

print("\n---------------")
print(orderItems.query('order_item_order_id == 2')[['order_item_id', 'order_item_order_id', 'order_item_subtotal']])

print("\n---------------")
print(orderItems.query('order_item_order_id == 2')['order_item_subtotal'].sum())

print("\n---------------")
print(orderItems.groupby(['order_item_order_id'])['order_item_subtotal'].sum())