from btree import Btree
from numpy import random
'''
Test the Btree
'''

NUM = 10

lst = []

while len(lst)!=NUM:
    new_v = random.randint(100)
    if new_v not in lst:
        lst.append(new_v)

bt = Btree(3)

for ind, el in enumerate(lst):
    bt.insert(el, ind)

print(lst)
bt.plot()
