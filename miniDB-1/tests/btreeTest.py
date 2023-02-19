from ..db.btree import Btree
from random import randrange
import sys

import os
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin/'

'''
Test the Btree
'''

NUM = int(sys.argv[1])
B = int(sys.argv[2])

lst = []

while len(lst)!=NUM:
    new_v = randrange(100)
    if new_v not in lst:
        lst.append(new_v)

bt = Btree(B)

for ind, el in enumerate(lst):
    bt.insert(el, ind)

print(lst)
bt.plot()
