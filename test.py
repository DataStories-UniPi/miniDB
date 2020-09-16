#!/usr/bin/python3

from qpdb import Table

print("Load from file (truth.pkl)")
truth = Table(load="truth.pkl")

print("Create with more types than columns")
try:
    tab = Table(name="TestTable", column_names=["Name", "Age", "Sex"], column_types=[str, int, str,str])
except:
    print('[+] Error cought')

print("Create empty")

tab = Table()

print("Create with attributes")
tab = Table(name="TestTable", column_names=["Name", "Age", "Sex"], column_types=[str, int, str])

print("Insert 4 correct rows")

tab.insert(["George", 23, "Male"])
tab.insert(["John", 21, "Male"])
tab.insert(["Anna", 20, "Female"])
tab.insert(["Lisa", 19, "Female"])

print("Show 4 rows")

tab.show()

print("Insert row with more records")

try:
    tab.insert(["George", 23, "Male", "Tall"])
except:
    print('[+] Error cought')

print("Insert row with wrong type")

try:
    tab.insert(["George", "tall", "Male"])
except:
    print('[+] Error cought')

try:
    print("Delete row 0")
except:
    print('[+] Error cought')

tab.delete(0)

print("Show #")

tab.show()

print("Select row 1 (load from dict) and show")

slct = tab.select(1)
slct.show()

print("Save current")

tab.save("testtab.pkl")

print("Load and test with truth.pkl")

testtab = Table(load="testtab.pkl")

if testtab.data == truth.data:
    print('[+] Success')
else:
    print('[-] FAILED')
