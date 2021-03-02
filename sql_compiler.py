from database import Database
import re
import sys

def create_table_func(querystring): #Function to create a table based on the query
    createtbregex = "(create)\s(table)\s([A-Za-z_0-9]+)\(([A-za-z_0-9]+\s(str|int)(,\s[A-za-z_0-9]+\s(str|int))*)\)"
    splitcreatetb = re.split(createtbregex, querystring) #Get a list whose elements are the matched patterns from the query and the regular expression made to detect a create sql command
    cleanstring = splitcreatetb[4].replace(',', '') #Trim the element contaning the column name and column types so their values can be inserted in to two different lists
    tbargs = cleanstring.split(" ")#Seperate the name of the column from the type of the column
    columns = []
    column_type = []
    for i in range(len(tbargs)):#Append the name or the type of the column to the approptiate list
        if (i % 2 == 0):
            columns.append(tbargs[i])
        else:
            if(tbargs[i] == "str"):
                column_type.append(str)
            elif(tbargs[i] == "int"):
                column_type.append(int)
    try:
        db.create_table(splitcreatetb[3], columns, column_type)
    except:
        print("Table creation was not successfull!")

def insert_func(querystring):
        insertregex = "(insert)\s(into)\s([A-Za-z0-9_]+)\s(values)\s?\(([A-Za-z0-9_]+(,\s[A-za-z_0-9]+)*)\)"
        splitinsert = re.split(insertregex, querystring)
        cleanstring = splitinsert[5].replace(',', '')
        splitargs = cleanstring.split(" ")
        try:
            db.insert(splitinsert[3], splitargs)
            print(f"Inserted row {splitargs} in table {splitinsert[3]}")
        except:
            print("Insertion command was not successfull!")

db = Database("ExampleDB", load=False)
create_table_func("create table sampleppl(name str, height int, weight int, age int)")
insert_func("insert into sampleppl values (Jim, 190, 90, 30)")
insert_func("insert into sampleppl values (Anna, 167, 58, 25)")
print("A sample database has been created with name 'ExampleDB'\none table named 'sampleppl' with the following columns(name str, height int, age int).\n", 
"and the following rows:\n",
"row1 (Jim, 190, 90, 30)\n",
"row2 (Anna, 167, 58, 25)")
print("Switch to another existing database using 'load database database_name' command.")
while (True):
    query = input("Enter an sql query to be executed or 'exit' to quit: \n")
    if(query.split(" ")[0] == ("select")):
        #selectregex = (select)\s*(\*?|([A-Za-z_]+,?\s?)+)\s*(from)\s*([A-Za-z_]+)\s*(where [A-Za-z_]+(<=|<|==|>=|>\d+))?\s*(top=(\d)+)?\s*(order by ([A-Za-z_]+ (asc|desc)))?\s*(save as ([A-Za-z_]+)+)?
        selectregex = "(select) (\*?|([A-Za-z_]+,?\s?)+) (from) ([A-Za-z_]+)?"
        splitselect = re.split(selectregex, query)
        if(splitselect[1] == "select" and splitselect[2] == '*'):
            try:
                db.select(splitselect[5], splitselect[2])
            except:
                print("Columns could not be selected due to an error!")
        elif(splitselect[1] == "select" and splitselect[2] != '*'):
            splitargs = splitselect[2].split(", ")
            try:
                db.select(splitselect[5], splitargs)
            except:
                print("Columns could not be selected due to an error!")
    elif(query.split(" ")[0] == ("update")):
        updateregex = "(update)\s([A-Za-z0-9_]+)\s(set)\s(([A-Za-z0-9_]+)[<=>]*(\"?[A-Za-z0-9_]+\"?))\s(where)\s([A-Za-z0-9_]+[<=>]*[A-Za-z0-9_]+)"
        splitupdate = re.split(updateregex, query)
        trimsplit1 = splitupdate[6].replace("\"",'')
        trimsplit2 = splitupdate[8].replace("\"", '')
        if('\"' in splitupdate[6]):
            try:
                db.update(splitupdate[2], trimsplit1, splitupdate[5], trimsplit2)
                print(f"Column {splitupdate[5]} has been updated with value: {trimsplit1}")
            except:
                print("Update query was not successfull!")
        else:
            try:
                db.update(splitupdate[2], int(trimsplit1), splitupdate[5], trimsplit2)
                print(f"Column {splitupdate[5]} has been updated with value: {trimsplit1}")
            except:
                print("Update query was not successfull")
    elif(query.split(" ")[0] == "create" and query.split(" ")[1] == "database"):
        createdbregex = "(create)\s(database)\s([A-Za-z_0-9]+)"
        splitcreatedb = re.split(createdbregex, query)
        dbname = splitcreatedb[3]
        try:
            db = Database(dbname, load = False)
            print(f"Database {dbname} was created.")
        except:
            print("Database creation was not successfull!")
    elif(query.split(" ")[0] == "create" and query.split(" ")[1] == "table"):
        create_table_func(query)
    elif(query.split(" ")[0] == "insert"):
        insert_func(query)
    elif(query.split(" ")[0] == "delete"):
        deleteregex = "(delete)\s(from)\s([A-Za-z0-9_]+)\s(where)\s([A-za-z0-9_]+\s?[<=>]*\s?[A-za-z0-9_]+)"
        splitdelete = re.split(deleteregex, query)
        try:
            db.delete(splitdelete[3], splitdelete[5])
        except:
            print("Deletion was not successfull!")
    elif(query.split(" ")[0] == "drop" and query.split(" ")[1] == "table"):
        droptbregex = "(drop)\s(table)\s([A-Za-z_0-9]+)"
        splitdroptb = re.split(droptbregex, query)
        try:
            db.drop_table(splitdroptb[3])
            print(f"Table {splitdroptb[3]} was dropped.")
        except:
            print("Table could not be dropped due to an error!")
    elif(query.split(" ")[0] == "load" and query.split(" ")[1] == "database"):
        loaddbregex = "(load)\s(database)\s([A-Za-z0-9_]+)"
        splitloaddb = re.split(loaddbregex, query)
        try:
            db = Database(splitloaddb[3], load = True)
        except:
            print("Database could not be loaded due to an error!")
    elif(query.split(" ")[0] == "help"): # Print a help message for the user to see
        print("Currently supported sql commands are: \n ",
        "select columns/* from table_name\n",
        "update table_name set value where condition\n",
        "insert into table_name values (value1, value2, ...)\n",
        "delete from table_name where condition\n",
        "create table table_name(column_name1 column_type1, column_name2 column_type2, ...)\n",
        "drop table table_name\n",
        "create database database_name\n",
        "load database database_name"
        )
    elif(query.split(" ")[0] == "exit"):
        sys.exit(0)
    else:
        print("Invalid sql syntax!\ntype 'help' to see a list of available supported commands and their syntax or 'exit' to quit.")