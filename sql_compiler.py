from database import Database
import vsmdb
import re
#vsmdb.db.select('classroom', ['building', 'capacity'])
searchquery = "select * from classroom" #input("Enter a command to execute:")
updatequery = "update classroom set capacity=39 where room_number==120"
searchregex = "(select) (\*?|([A-Za-z_]+,?\s?)+) (from) ([A-Za-z_]+)?"
updateregex = "(update)\s([A-Za-z0-9_]+)\s(set)\s(([A-Za-z0-9_]+)[<=>]*([A-Za-z0-9_]+))\s(where)\s([A-Za-z0-9_]+[<=>]*[A-Za-z0-9_]+)"
splitsearch = re.split(searchregex, searchquery)
splitupdate = re.split(updateregex, updatequery)
print(splitsearch)
print(splitupdate)
splitargs = splitsearch[2].split(", ")
print(splitargs)
for arguments in splitargs:
    print (arguments)
if(splitsearch[1] == "select" and splitsearch[2] == '*'):
    vsmdb.db.select(splitsearch[5], splitsearch[2])
elif(splitsearch[1] == "select" and splitsearch[2] != '*'):
    vsmdb.db.select(splitsearch[5], splitargs)
if(splitupdate[1] == "update"):
    vsmdb.db.update(splitupdate[2], int(splitupdate[6]), splitupdate[5], splitupdate[8])
vsmdb.db.select('classroom', '*')

#(select)\s*(\*?|([A-Za-z_]+,?\s?)+)\s*(from)\s*([A-Za-z_]+)\s*(where [A-Za-z_]+(<=|<|==|>=|>\d+))?\s*(top=(\d)+)?\s*(order by ([A-Za-z_]+ (asc|desc)))?\s*(save as ([A-Za-z_]+)+)?
#safe (select) (\*?|([A-Za-z_]+,?\s?)+) (from) ([A-Za-z_]+)