

class Foo:
    pass


foo = Foo()
foo.x = 1

setattr(foo, 'hello world!', 'Hello World!')
print(dict(vars(foo)))

print(getattr(foo, 'hello world!'))

setattr(foo, 38, 'Goodbye')
