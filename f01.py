import numpy
import ssl; 

print(ssl.get_default_verify_paths())

c = {'a','b'}
a = ['a','b']
c.pop('1')
a.append('c')
for x in a:
    print(x)
