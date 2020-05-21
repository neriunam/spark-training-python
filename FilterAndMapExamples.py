import functools as ft

list = range(1, 6)

print("All elements list:")
print(list)

# Filter for select only even values
evenNumbers = filter(lambda x: x%2 == 0, list)
oddNumbers = filter(lambda x: x%2 != 0, list)
print("Even numbers")
print(evenNumbers)
print("Odd numbers")
print(oddNumbers)

# Map function
lsqr = map(lambda x: x*x, evenNumbers)
print("Even numbers square")
print(lsqr)

print(type(evenNumbers))

# Reduce example 1,2,3,4,5,6,7,8,9,10
red = ft.reduce(lambda x, y: x + y, list)

print(red)
