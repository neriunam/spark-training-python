
# This is a comment in python

# Multiline string
s = """This is a
multiline string
in
python"""
print("Hello world")
print(s)

# Multiple Assignment
i = counter = 0
print(i, counter)

# Assign multiple values
a, b, c, s = 1, 2, 3, "hello"
print(a, b, c, s)

# Number types
i = 10          # Integer
f = 10.2        # Float
c = 12.42 + 12j    # Complex
print(i, f, c)

# List
list1 = [ "one", "two", "three", "four" ]
print(list1)
print(list1[0:1])   # Print first element
print(list1[1:2]*3) # Print second element repetead 3 times

# Set
set1 = { "one", "two", "three", "two" } # Set doesn't allow duplicates
print(set1)

# Dictionary
print("Dictionary example")
dict1 = { 1:"one", 2:"two", 3:"three" }
print(dict1)
print(len(dict1))

# Tuples
print("\nTuple example")
tuple1=('john','123','25.68','456')
print(type(tuple1))
print(len(tuple1))
print(tuple1)

# Loops
for number in list1:
    if number == "one":
        print(1)
    elif number == "two":
        print(2)
    elif number == "three":
        print(3)
    else:
        print(4)

# Try -> except
s = "This is not int, is a string"
try:
    int(s)
except ValueError:
    print(s + " IS NOT int")
finally:
    print("finally block for cleanup actions")

