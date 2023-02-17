print("-- Test hash Indexing --")
import sys
sys.path.append('miniDB')

from hash_ import Hash

h = Hash(2)

h.insert(4.6,4)
h.insert(24,24)
h.insert(16,16)
h.insert(6,6)
h.insert(22,22)
h.insert(10,10)
h.insert(7,7)
h.insert(131, 131)
h.insert(231,231)
h.insert(20, 20)
h.insert(26, 26)
h.insert(331, 331)
h.insert(0,0)
h.insert(10000,10000)
h.insert(20000,20000)
h.insert(30000,30000)
h.insert(1,1)
h.insert(10001,10001)
h.insert(20001, 20001)
h.insert(30001,30001)

h.insert(10000000, 10000000000)
h.insert(20000000, 20000000000)
h.insert(30000000, 30000000000)
h.insert(40000000, 40000000000)
h.insert(50000000, 50000000000)
print(h.find(4.6))
h.show()


