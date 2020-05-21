# Function
def compare(x, y):
    s =  ""
    if x > y:
        s = str(x) + " is great than " + str(y)
    elif x < y:
        s = str(x) + " is less than " + str(y)
    else:
        s = str(x) + " is equal than " + str(y)
    return s

print(compare(2, 4))

# Function with lambda parameter
def operation(x, y, op):
    return op(x, y)

# Call function with lambda
print(operation(4, 8,  lambda x, y: x + y))
print(operation(4, 8,  lambda x, y: x * y))

