def duplicate(i):
    return i * 2


def square(i):
    return i * i


def operation(top, f):
    print("\nStart 1, top: " + str(top) + ", function: " + str(f))
    for i in range(1, top + 1):
        print(i, f(i))


operation(5, lambda i: duplicate(i))

operation(5, lambda i: square(i))

