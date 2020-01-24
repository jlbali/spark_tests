
# Type hints examples.
# https://docs.python.org/3/library/typing.html

def basic():
    print("Basic PyCharm test")

def concat_string(name1:str, name2:str) -> str:
    return name1 + "," + name2

def basic2():
    print(concat_string("Hello ", "Typing World!"))

