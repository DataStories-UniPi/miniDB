from database import Database
import vsmdb
import re
#vsmdb.db.select('classroom', ['building', 'capacity'])
searchquery = "select * from classroom" #input("Enter a command to execute:")
updatequery = "update classroom set capacity=39 where room_number==120"
createdbquery = "create database test_db"
createtbquery = "create table professors1(salary int, name str, age int)"
droptbquery = "drop table professors1"
insertquery = "insert into professors1 values (50, takhs, 87)"
searchregex = "(select) (\*?|([A-Za-z_]+,?\s?)+) (from) ([A-Za-z_]+)?"
updateregex = "(update)\s([A-Za-z0-9_]+)\s(set)\s(([A-Za-z0-9_]+)[<=>]*([A-Za-z0-9_]+))\s(where)\s([A-Za-z0-9_]+[<=>]*[A-Za-z0-9_]+)"
createdbregex = "(create)\s(database)\s([A-Za-z_0-9]+)"
createtbregex = "(create)\s(table)\s([A-Za-z_0-9]+)\(([A-za-z_0-9]+\s(str|int)(,\s[A-za-z_0-9]+\s(str|int))*)\)"
droptbregex = "(drop)\s(table)\s([A-Za-z_0-9]+)"
insertregex = "(insert)\s(into)\s([A-Za-z0-9_]+)\s(values)\s?\([A-Za-z0-9_]+(,\s[A-za-z_0-9]+)*\)"
splitsearch = re.split(searchregex, searchquery)
splitupdate = re.split(updateregex, updatequery)
splitcreatedb = re.split(createdbregex, createdbquery)
splitcreatetb = re.split(createtbregex, createtbquery)
splitdroptb = re.split(droptbregex, droptbquery)
splitinsert = re.split(insertregex, insertquery)
print(splitsearch)
splitargs = splitsearch[2].split(", ")
print(splitargs)
for arguments in splitargs:
    print (arguments)
print(splitupdate)
print(splitcreatedb)
print(splitcreatetb)
print(splitdroptb)
print(splitinsert)
if(splitsearch[1] == "select" and splitsearch[2] == '*'):
    vsmdb.db.select(splitsearch[5], splitsearch[2])
elif(splitsearch[1] == "select" and splitsearch[2] != '*'):
    vsmdb.db.select(splitsearch[5], splitargs)
if(splitupdate[1] == "update"):
    vsmdb.db.update(splitupdate[2], int(splitupdate[6]), splitupdate[5], splitupdate[8])
vsmdb.db.select('classroom', '*')
if(splitcreatedb[1] == "create" and splitcreatedb[2] == "database"):
    dbname = splitcreatedb[3]
    testdb = Database(dbname, load = False)
    testdb.create_table('test_table', ['name', 'height', 'weight'], [str, int, int])
    testdb.insert('test_table', ['Bob', 190, 80])
    testdb.insert('test_table', ['Patric', 170, 60])
    testdb.select('test_table', '*')
if(splitcreatetb[1] == "create" and splitcreatetb[2] == "table"):
    cleanstring = splitcreatetb[4].replace(',', '')
    tbargs = cleanstring.split(" ")
    columns = []
    column_type = []
    print(tbargs)
    for i in range(len(tbargs)):
        print(tbargs[i])
        if (i % 2 == 0):
            columns.append(tbargs[i])
        else:
            if(tbargs[i] == "str"):
                column_type.append(str)
            elif(tbargs[i] == "int"):
                column_type.append(int)
    print(columns)
    print(column_type)
    testdb.create_table(splitcreatetb[3], columns, column_type)
    testdb.insert('professors1', [100, 'Patric', 47])
    testdb.insert('professors1', [170, 'Bob', 35])
    testdb.tables[splitcreatetb[3]]
    testdb.select('professors1', '*')
if(splitdroptb[1] == "drop" and splitdroptb[2] == "table"):
    testdb.drop_table(splitdroptb[3]) 
#(select)\s*(\*?|([A-Za-z_]+,?\s?)+)\s*(from)\s*([A-Za-z_]+)\s*(where [A-Za-z_]+(<=|<|==|>=|>\d+))?\s*(top=(\d)+)?\s*(order by ([A-Za-z_]+ (asc|desc)))?\s*(save as ([A-Za-z_]+)+)?
#safe (select) (\*?|([A-Za-z_]+,?\s?)+) (from) ([A-Za-z_]+)
#createtable (create)\s(table)\s([A-Za-z_0-9]+)\(([A-za-z_0-9]+\s(str|int)(,\s[A-za-z_0-9]+\s(str|int))*)\)
#insert (insert)\s(into)\s([A-Za-z0-9_]+)\s(values)\s\([A-Za-z0-9_]+(,\s[A-za-z_0-9]+)*\)