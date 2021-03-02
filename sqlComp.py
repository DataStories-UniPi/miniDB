from database import Database
#import platform

#print(platform.system())

print("\n\n********************************************************")
print("***************SQL to miniDB interpreter****************")
print("********************************************************\n\n")


global SQL_dict  # SQL Dictionary
SQL_dict = ['SELECT', 'CREATE', 'INSERT', 'UPDATE', 'DELETE',
'DROP', 'LOAD', 'INNER', 'JOIN', 'ON', 'WHERE', 'FROM', 'ORDER',
'BY', 'IN', 'INTO', 'TABLE', 'DATABASE', 'SET', 'ALTER', 'INDEX', 'ASC', 'DESC'
, 'VALUES', 'SET', 'ALTER']

global db_load
db_load = 0

#-------------------------------------------------------------------------------
#-----------------------FUNCTIONS-----------------------------------------------
#-------------------------------------------------------------------------------
def uppercaseSQLkeyWords(qsplit):

    for i in range(len(qsplit)):
        if qsplit[i].upper() in SQL_dict:
            qsplit[i] = qsplit[i].upper()
    #print("\n\n--upper case Query: ", qsplit)
    return qsplit



def basicSyntaxCheck(qsplit):  # basic syntax check for valid startup SQL command
    sql_start = ['SELECT', 'CREATE', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'LOAD']
    if (qsplit[-1][-1]!=';') or (qsplit[0].upper() not in sql_start):
        return 0
    else:
        return 1



def is_SQL(str):
    str = str.upper()
    if str in SQL_dict:
        return 0
    return 1



def delQuestionMark(qsplit):
    if q_split[-1]==';':  # remove ';' if it is the last element of qsplit list
        q_split.remove(';')
        return qsplit
    else:
        q_split[-1] = q_split[-1][0:-1]  # remove ';' if it is the last char of the last element of qsplit list
        return qsplit



def executeOperation(qsplit, db):  # Called after basicSyntaxCheck and executes the operation
    op_dict = {'SELECT': SELECT, 'INSERT': INSERT, 'DELETE': DELETE}  # operation dictionary
    flag = op_dict[qsplit[0]](qsplit, db)  # return operation

    return flag

#-------------------------------------------------------------------------------
#--------------------CONDITION CHECK ROUTINES-----------------------------------
#-------------------------------------------------------------------------------
#------------splitCondition()---------------------------------------------------
def splitCondition(condition):  # split Condition on OPERATOR
    ops = ['<', '>', '=', '!']

    if condition[0] in ops:  # return 0 if Condition starts with an operator
        return 0

    for i in range(1, len(condition)):  # return split[left, right, operator]
        if condition[i] in ops:
            if i!=len(condition)-1:
                if condition[i]=='=':
                    split = condition.split('=', 1)
                    split.append('==')
                    return split

                if condition[i+1]=='=':
                    split = condition.split(condition[i]+'=', 1)
                    split.append(condition[i]+'=')
                    return split

                if condition[i]!='!':
                    split = condition.split(condition[i], 1)
                    split.append(condition[i])
                    return split

    return 0
#------------------End of splitCondition()--------------------------------------
#-------------------------------------------------------------------------------
def splitTableColumn(str):
    spl_list = []
    split_list = str.split('.')  # split on '.' to separete the Tabe_name from Column_name

    if len(split_list)==1:  # no '.' to split on. str = Column_name
        return split_list

    if len(split_list)==2:
        spl_list.append(split_list[1])  # append Column_name to spl_list[0]
        spl_list.append(split_list[0])  # append Table_name to spl_list[1]
        return spl_list

    return 0  # if len(split_list)>2 means multiple '.' inside the str, invalid condition segment

#-----------parseCondition()----------------------------------------------------
def parseCondition(qsplit, idx, table1, table2=None, cond_name='WHERE'):  # parseCondition is called only after 'WHERE' or 'INNER JOIN ON' clauses
    '''
    a Condition cannot include spaces among operator and operands. However, the below for-loop
    scans for up to 4 qsplits (in case Condition is like: (a > = b) ). As a result, future support
    for a less strict Condition syntax can be applied for len(condition) > 1 .

    '''
    c_str_idx = idx  # Condition's starting point index ( if exists )
    condition = []
    for i in range(c_str_idx, len(qsplit)):
        if qsplit[i] in SQL_dict or len(condition)==4:   # Condition split cannot contain SQL clauses or have length bigger that four
            break
        condition.append(qsplit[i])

    condLen = len(condition)  # Condition list length
    #print(f'\n--condition list: {condition}')

    # if the given Condition split length = 0  ex. SELECT * FROM table1 WHERE -
    if condLen==0:  # Condition segment cannot be zero
        print(f"\n\n--Invalid syntax: Condition missing in '{cond_name}' clause--")
        return 0

    # if the give Condition split length = 1 ( input Condition doesn't include spaces )
    #ops = ['>=', '<=', '=', '>', '<', '!=']

    if condLen==1:  # Condition split length = 1 since Condition cannot include spaces

        split = splitCondition(condition[0])
        print(f'DEBUG info--splitCondition(condition[0]): {split}')  # DEBUG
        if not split:
            print(f"\n\n--Invalid syntax: Condition segment error in '{cond_name}' clause--")
            return 0

        spl = splitTableColumn(split[0])
        print(f"DEBUG info--splitTableField(split[0]):    {spl}")  # DEBUG


        if spl==0 or not checkTableColumnSyntax(split[0]):
            print(f"\n\n--Invalid syntax: Column syntax error under Condition segment in '{cond_name}' clause--")
            return 0


        if len(spl)==1:
            return [split[0]+split[2]+split[1], 1]  # Column + operator + Value (1 is for check in INNER JOIN since table.column is only valid)

        if len(spl)==2:
            if spl[1]!=table1 and spl[1]!=table2:
                print(f"\n\n--Possible error: in Condition, Table '{spl[1]}'", f"doesnt match any given Table in '{cond_name}' clause--")
                return 0


            return [spl[0]+split[2]+split[1], spl[1]]  # Column + operator + Value (+ table_name of the column)

    else:
        print(f"\n\n--Invalid syntax: Condition segment cannot inlcude spaces or multiple '.' in '{cond_name}' clause--")
        return 0

    return 1
#---------End of parseCondition()-----------------------------------------------

#----check_ORDER_BY-------------------------------------------------------------
def check_ORDER_BY(qsplit, idx, table1, table2=None):  # checks for ORDER BY clause after a Condition
    if len(qsplit)<idx+3 or len(qsplit)>idx+4 or qsplit[idx+1]!='BY':
        print("\n\n--Invalid syntax: 'ORDER BY' error--")
        return 0

    order_col = splitTableColumn(qsplit[idx+2])  # get the ORDER BY Column [col, table]
    print(f"DEBUG info--splitTableColumn() in check_ORDER_BY(): {order_col}")

    if order_col==0 or not checkTableColumnSyntax(order_col[0].upper()):
        print("\n\n--Invalid syntax:  in (ORDER BY) Column name syntax NOT valid --")
        return 0


    if len(order_col)==2 and not checkTableColumnSyntax(order_col[1].upper()):
        print(f"\n\n--Invalid syntax: in (ORDER BY) Table_name '{order_col[1]}' not valid--")
        return 0


    #  return 0 if table.column in ORDER BY doesnt match the given Table1
    if order_col!=0 and len(order_col)==2 and order_col[1]!=table1 and order_col[1]!=table2:
        print(f"\n\n--Possible error: in (ORDER BY) Table '{order_col[1]}' doesn't match any given Table'")
        return 0


    if qsplit[-1]=='ASC':
        return [order_col, 'ASC']   # return: [ [colName, tableName], 'ASC' ]

    if qsplit[-1]=='DESC':
        return [order_col, 'DESC']  # return: [ [colName, tableName], 'DESC' ]
    else:
        if qsplit[-2]!='BY':
            print(f"\n\n--Invalid syntax:  in (ORDER BY) syntax error - 'ASC' or 'DESC' expected after '{qsplit[-2]}'--")
            return 0

    return [order_col, ""]  # return: [ [colName, tableName], [] ]
#------------End of check_ORDER_BY()--------------------------------------------


# check table1 syntax - table name cannot be numeric nor contain non alphanumeric chars nor be an SQL clause
def checkTableColumnSyntax(table):
    if not table in SQL_dict and not table.isnumeric():
        for i in table:
            if table[0]=='.' or table[-1]=='.' or (not i.isalpha() and not i=='_' and not i=='.'):
                return 0
        return 1
    return 0
#------------------End of checkTableColumnSyntax()------------------------------
#-------------------------------------------------------------------------------
#--------------------------getColumns()-----------------------------------------
def getColumns(qsplit):
    #  Columns segment always lies between 'SELECT' and 'FROM' clauses
    cols = []
    if qsplit[2]=='FROM' and qsplit[1]=='*':
        cols.append('*')
        return cols

    for i in qsplit[1:]:  # scan for Columns starts after qsplit[0]='SELECT'
        if i=='FROM':
            break
        if not checkTableColumnSyntax(i):
            print(f"\n\n--Invalid syntax: Column '{i}' syntax is not valid--")
            return 0

        cols.append(i)

    print(f"DEBUG info--getColumns(qsplit):           {cols}")
    return cols
#-------------------------End of getColumns()-----------------------------------
#-------------------------------------------------------------------------------
#-------------------------parseColumns()----------------------------------------
def parseColumns(csplit, table1, table2=None):
    #  take column list from getColumns() and for each split parses the table_name and column_name
    cols = []
    tables= []

    for i in csplit:
        split = splitTableColumn(i)  # split its column to [column_name, table_name]
        if split==0:
            print(f"\n\n--Invalid syntax: Column '{i}' syntax is not valid, multiple '.' found--")
            return 0

        if len(split)==1:
            cols.append(split[0])  # split[0] = Column_name
            tables.append(1)       # split[1] = Table_name

        if len(split)==2:

            if split[1]!=table1 and split[1]!=table2:
                print("table1: ", table1, " table2: ", table2, " split[1]: ", split[1])
                print(f"\n\n--Possible error: in COLUMNS, Table '{split[1]}' doesn't match any given Table'--")
                return 0

            cols.append(split[0])     # split[0] = Column_name
            tables.append(split[1])   # split[1] = Table_name

        if len(split)>2:
            print(f"\n\n--Invalid syntax: Column '{i}' syntax is not valid--")
            return 0

    # [ ['Col_1', 'Col_2', ...], ['Table_1', 'Table2', ...] ] - list of lists
    print("DEBUG info--parseColumns():              ", [cols, tables])
    return [cols, tables]
#--------------------------End of parseColumns()--------------------------------
#-------------------------------------------------------------------------------

#------------------------parseColumnsINNER_JOIN()-------------------------------
def parseColumnsINNER_JOIN(csplit, tsplit):  # csplit = Columns, tsplit = Tables

    length = len(csplit)
    cols = []

    if csplit[0]=='*':
        return ['*']

    for i in range(length):
        if tsplit[i]==1:
            return 0

        col = tsplit[i] + '_' + csplit[i]  # miniDB Column format for INNER JOIN:  TableName_ColumnName
        cols.append(col)

    print(f"DEBUG info--parseColumnsINNER_JOIN():     {cols}")
    return cols  # returns: [ tableName_columnName ]

#-------------------End of parseColumnsINNER_JOIN()-----------------------------
#--------------------------OPERATION FUNCTIONS----------------------------------
#-------------------------------------------------------------------------------
# -------------------SELECT * FROM table1 (ORDER BY col)------------------------
def selectFromOrderBy(qsplit, csplit, table1, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0

    if len(qsplit)==len(csplit)+3:  # table1 has already been checked for valid syntax before the function call
        if csplit[0]=='*':
            del csplit
            csplit = '*'
        # csplit is the Column list: if '*' is given then csplit='*' else it remains as is.
        try:
            db.select(table1, csplit)
        except KeyError:
            print(f"\n\n\n!!!-----miniDB EXCEPTION Table '{table1}' not found------!!!")
        return 1

    # SELECT * FROM table1 ORDER BY column
    if qsplit[len(csplit)+3]=='ORDER':  # check_ORDER returns a list with the Column to order by and ASC or DESC
        flag = check_ORDER_BY(qsplit, len(csplit)+3, table1)

        if flag == 0:
            return 0

        order = False

        if flag[-1] =='ASC':
            order = True

        if csplit[0]=='*':
            del csplit
            csplit = '*'

        try:
            db.select(table1, csplit, order_by=flag[0][0], asc=order)  # call to miniDB select function
        except ValueError:
            print(f'\n\n--Column "{table1}" not found--')
            db.unlock_table(table1)
        return 1

    return 2
#---------------------End of selectAllFromOrderBy()-----------------------------
#-------------------------------------------------------------------------------
#---------------selectAllFromWhereOrderBy()-------------------------------------
def selectFromWhereOrderBy(qsplit, csplit, table1, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0

    condition = parseCondition(qsplit, len(csplit)+4, table1)
    if not condition:
        return 0
    # SELECT * FROM table1 WHERE condition
    if len(qsplit)==len(csplit)+5:  # no 'ORDER BY' clause following
        if csplit[0]=='*':
            del csplit
            csplit = '*'

        try:
            db.select(table1, csplit, condition[0])
        except KeyError:
            print(f"\n\n\n!!!-----miniDB EXCEPTION 'Table {table1}not found'------!!!")
        except ValueError:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
        except Exception:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
        return 1

    # SELECT * FROM table1 WHERE conditio ORDER BY column
    if qsplit[len(csplit)+5]=='ORDER':
        flag = check_ORDER_BY(qsplit, len(csplit)+5, table1)  # flag = [ columnName, tableName ]

        if flag == 0:
            return 0

        order = False

        if flag[-1] =='ASC':
            order = True

        if csplit[0]=='*':
            del csplit
            csplit = '*'
        print(f"DEBUG info--check_ORDER_BY:               {flag[0]}")
        try:
            db.select(table1, csplit, condition[0], order_by=flag[0][0], asc=order)  # call to miniDB select function
        except ValueError:
            print(f'\n\n--Column "{table1}" not found--')
        except Exception:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
            db.unlock_table(table1)
        return 1

    return 2
#---------------------End of selectAllFromWhereOrderBy()------------------------
#-------------------------------------------------------------------------------
#----------------------------------INNER JOIN-----------------------------------
#-------------------------------------------------------------------------------
#-----------------------------selectAllInnerJoinOn------------------------------
def selectINNER_JOIN_OrderBy(qsplit, csplit, tbsplit, table1, table2, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0


    InnerJoin_condition = parseCondition(qsplit, len(csplit)+7, table1, table2, cond_name='JOIN ON')
    if not InnerJoin_condition:  # False means that parseCondition() is called after 'INNER JOIN ON'
        return 0  # return 0 if Condition segment not valid

    # query like: SELECT [Cols] FROM table1 INNER JOIN table2 ON Condition
    if len(qsplit)==len(csplit)+8:
        if csplit[0]=='*':
            del csplit
            csplit = '*'
        # csplit is the Column list: if '*' is given then csplit='*' else it remains as is.
        try:
            print(f"DEBUG info--INNER JOIN ON--Condition:     {InnerJoin_condition[0]}")
            db.inner_join(table1, table2, InnerJoin_condition[0], return_object=True)._select_where(csplit).show()  # miniDB Command
        except KeyError:
            print(f"\n\n\n!!!-----miniDB EXCEPTION Table '{table1}' not found------!!!")
        except ValueError:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
        except Exception:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
        return 1


    # SELECT ... FROM table1 INNER JOIN table2 ON Condition ORDER BY (ASC, DESC)
    if qsplit[len(csplit)+8]=='ORDER':
        flag = check_ORDER_BY(qsplit, len(csplit)+8, table1, table2)  # flag = [ [colName, tableName ], 'ASC' or 'DESC' or "" ]
        print("--flag: ", flag)
        if flag == 0:
            return 0

        if len(flag[0])==1:
            print("\n\n--Possible error: Wrong Column format on (ORDER BY) clause. Format must be like  'tableName.colName'--")
            return 0  # return 0 if ORDER BY column is of not valid format

        order = False

        if flag[-1] =='ASC':
            order = True

        parsedFlag = parseColumnsINNER_JOIN([flag[0][0]], [flag[0][1]])  # parsedFlag = tableName_colName

        print("\n\n--parsedFlag: ", parsedFlag)
        if csplit[0]=='*':
            del csplit
            csplit = '*'
        print(f"DEBUG info--check_ORDER_BY:               {flag}")
        try:
            print(f"DEBUG info--INNER JOIN ON--Condition:     {InnerJoin_condition[0]}")
            db.inner_join(table1, table2, InnerJoin_condition[0], return_object=True)._select_where(csplit, order_by=parsedFlag[0], asc=order).show()  # miniDB Command
        except ValueError:
            print('\n\n--miniDB Value Error exeption. Possible wrong column or condition--')
        except Exception:
            print("\n\n--miniDB Exception Error exeption. Possible wrong column or condition--")
            db.unlock_table(table1)
        return 1

    return 2
#---------------------------End of selectFromInnerJoinOrderBy()-----------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
def selectINNER_JOIN_Where_OrderBy(qsplit, csplit, tbsplit, table1, table2, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0

    #  check the INNER JOIN ON Condition
    InnerJoin_condition = parseCondition(qsplit, len(csplit)+7, table1, table2, cond_name='JOIN ON')
    if not InnerJoin_condition:  # False means that parseCondition() is called after 'INNER JOIN ON'
        return 0  # return 0 if Condition segment not valid

    #  check the WHERE Condition
    WHERE_condition = parseCondition(qsplit, len(csplit)+9, table1, table2, cond_name='WHERE')
    if not WHERE_condition:  # False means that parseCondition() is called after 'INNER JOIN ON'
        return 0  # return 0 if Condition segment not valid


    if WHERE_condition[1]==1:
        print("\n\n--Possible error: Wrong Column format on (WHERE) condition. Format must be like  'tableName.colName'--")
        return 0  # return 0 if Condition column is of not valid format

    #  new_condition(string) = tableName + '_' + old_condition(string), ex: table1_column >= 4000
    WHERE_condition = WHERE_condition[1] + '_' + WHERE_condition[0]


    # query like: SELECT [Cols] FROM table1 INNER JOIN table2 ON Condition1 WHERE Condition2
    if len(qsplit)==len(csplit)+10:
        if csplit[0]=='*':
            del csplit
            csplit = '*'
        # csplit is the Column list: if '*' is given then csplit='*' else it remains as is.

        try:
            print(f"DEBUG info--INNER JOIN ON--Condition:     {InnerJoin_condition[0]}")
            print(f"DEBUG info--WHERE--Condition:     {WHERE_condition}")
            db.inner_join(table1, table2, InnerJoin_condition[0], return_object=True)._select_where(csplit, WHERE_condition).show()  # miniDB Command
        except KeyError:
            print(f"\n\n\n!!!-----miniDB EXCEPTION Table '{table1}' not found------!!!")
        except ValueError:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
        except Exception:
            print("\n\n--miniDB Value Error exeption. Possible wrong column or condition--")
        return 1



    # SELECT ... FROM table1 INNER JOIN table2 ON Condition ORDER BY (ASC, DESC)
    if qsplit[len(csplit)+10]=='ORDER':
        flag = check_ORDER_BY(qsplit, len(csplit)+10, table1, table2)  # flag = [ [colName, tableName ], 'ASC' or 'DESC' or "" ]
        print("--flag: ", flag)
        if flag == 0:
            return 0

        if len(flag[0])==1:
            print("\n\n--Possible error: Wrong Column format on (ORDER BY) clause. Format must be like  'tableName.colName'--")
            return 0  # return 0 if ORDER BY column is of not valid format

        order = False

        if flag[-1] =='ASC':
            order = True

        parsedFlag = parseColumnsINNER_JOIN([flag[0][0]], [flag[0][1]])  # parsedFlag = tableName_colName

        print("\n\n--parsedFlag: ", parsedFlag)
        if csplit[0]=='*':
            del csplit
            csplit = '*'
        print(f"DEBUG info--check_ORDER_BY:               {flag}")
        try:
            print(f"DEBUG info--INNER JOIN ON--Condition:     {InnerJoin_condition[0]}")
            db.inner_join(table1, table2, InnerJoin_condition[0], return_object=True)._select_where(csplit, WHERE_condition, order_by=parsedFlag[0], asc=order).show()  # miniDB Command
        except ValueError:
            print('\n\n--miniDB Value Error exeption. Possible wrong column or condition--')
        except Exception:
            print("\n\n--miniDB Exception Error exeption. Possible wrong column or condition--")
            db.unlock_table(table1)
        return 1

    return 2
#-------------------------------------------------------------------------------
#------------------------END OF selectINNER_JOIN_Where_OrderBy()----------------
#-------------------------------------------------------------------------------
'''
        -------------------- SELECT OPERATION ------------------
'''
#------------------------SELECT OPERATION group---------------------------------
def SELECT(qsplit, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0

    # SELECT * FROM, WHERE, INNER JOIN ON, ORDER BY
    print("\n\n--------SELECT Operation---------\n\n\n")
    qsplit_len = len(qsplit)  # query list length

    # SELECT * FROM table1
    if qsplit_len<4 or 'FROM' not in qsplit or qsplit[1]=='FROM':
        return 0  # invalid query length, too small or 'FROM' keyword missing
#-------------------------------------------------------------------------------
#--SELECT - FROM table1 (WHERE condition_string, INNER JOIN, ORDER BY)----------
#-------------------------------------------------------------------------------
    col_split = getColumns(qsplit)  # return a list with the given Columns for Select
    if not col_split:
        return 0
    #-----------------------------------------------------------------------
    # check Table1 for valid syntax
    table1 = qsplit[len(col_split)+2]

    if not checkTableColumnSyntax(table1):  # qsplit[len(c_split)+2]=table1
        print(f"\n\n--Invalid syntax: Table 1 '{table1}' syntax not valid--")
        return 0  # not valid query if table1 is a number or contains not alphanumeric chars

    #  seperate Column names from Table names in Column segment

    if len(qsplit)<len(col_split)+4 or qsplit[len(col_split)+3]!='INNER':
        parseCols = parseColumns(col_split, table1)  # do not run on INNER JOIN query
        if not parseCols:
            return 0

        c_split = parseCols[0]


        if qsplit[len(c_split)+1]!='FROM':  # if 'FROM' not in the right position, return 0
            return 0
        #---------------------------------------------------------------------------
        #--------SELECT ... FROM table1 (ORDER BY column)---------------------------
        table1 = qsplit[len(c_split)+2]
        flag = selectFromOrderBy(qsplit, c_split, table1, db)
        if flag==0:
            return 0  # invalid SQL query
        if flag==1:
            return 1  # valid SQL query
        #---------------------------------------------------------------------------
        #---------------------------------------------------------------------------
        '''
                -- WHERE --
        '''
        if qsplit[len(c_split)+3]=='WHERE':  # SELECT * FROM table_name WHERE Condition
            #-----------------------------------------------------------------------
            #--------SELECT ... FROM table1 WHERE Condition (ORDER BY column)-------
            flag = selectFromWhereOrderBy(qsplit, c_split, table1, db)
            if flag==0:
                return 0  # invalid SQL query
            if flag==1:
                return 1  # valid SQL query
            #-----------------------------------------------------------------------
            #-----------------------------------------------------------------------
        #------------End of SELECT - FROM WHERE ( ORDER BY )------------------------
    #---------------------------------------------------------------------------
    #-------------SELECT ... FROM tabl1 INNER JOIN tabl2 ON (WHERE, ORDER BY)---
    '''
            -- INNER JOIN -- ORDER BY --
    '''
    table1 = qsplit[len(col_split)+2]
    table2 = qsplit[len(col_split)+5]

    parseCols = parseColumns(col_split, table1, table2)
    if not parseCols:
        return 0

    c_split = parseCols[0]  # Columns list
    t_split = parseCols[1]  # Tables list
    #print(c_split)
    #print(t_split)

    if len(qsplit) < len(c_split)+8:
        return 0  # query length too small for 'INNER JOIN'

    if qsplit[len(c_split)+3]!='INNER' or qsplit[len(c_split)+4]!='JOIN' or qsplit[len(c_split)+6]!='ON':
        return 0  # return 0 if 'INNER JOIN ON' clause is missing


    #  check table2 for valid Syntax
    if not checkTableColumnSyntax(qsplit[len(c_split)+5]):  # qsplit[3]=table1
        print(f"\n\n--Invalid syntax: Table 2 '{qsplit[len(c_split)+5]}' syntax not valid--")
        return 0  # not valid query if table2 is a number or contains not alphanumeric chars

    '''
    the time selectAllInnerJoinOn() is called, table1 and table2 and INNER JOIN syntax are
    already successfully checked
    '''

    cols = parseColumnsINNER_JOIN(c_split, t_split)

    if not cols:
        print("\n\n--Possible error: Column format must be like 'tableName.colName'--")
        return 0

    flag = selectINNER_JOIN_OrderBy(qsplit, cols, t_split, table1, table2, db)
    if flag==0:
        return 0  # invalid SQL query
    if flag==1:
        return 1  # valid SQL query

    '''
          --- INNER JOIN ON WHERE -- ORDER BY ---
    '''

    if qsplit[len(c_split)+8]=='WHERE':  # SELECT * FROM table_name WHERE Condition
        #-----------------------------------------------------------------------
        #--------SELECT ... FROM table1 WHERE Condition (ORDER BY column)-------
        flag = selectINNER_JOIN_Where_OrderBy(qsplit, cols, t_split, table1, table2, db)
        if flag==0:
            return 0  # invalid SQL query
        if flag==1:
            return 1  # valid SQL query
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #--------End SELECT - FROM tabl1 INNER JOIN tabl2 ON (WHERE, ORDER BY)------
    #---------------------------------------------------------------------------
    return 1
#--------------------------End of SELECT()--------------------------------------
#-------------------------------------------------------------------------------
def LOAD(qsplit):

    print("\n\n--------LOAD DATABASE Operation---------\n")
    qsplit_len = len(qsplit)  # query list length

    if qsplit_len!=3 or qsplit[1]!='DATABASE':  # query length must equal 3

        return -1

    db_name = qsplit[2]
    if not checkTableColumnSyntax(db_name):
        print(f"\n\n--Invalid syntax: Database '{db_name}' name is  not valid--")
        return 0

    db = Database(db_name, 1)
    return db
#-------------------------------------------------------------------------------
#---------------------------End of LOAD Database--------------------------------
#-------------------------------------------------------------------------------
#------------------------INSERT-------------------------------------------------
def INSERT(qsplit, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0


    qsplit_len = len(qsplit)

    #  INSERT INTO table1 VALUES ...   minimun legal length = 5 (table has one column)
    if qsplit_len<5 or qsplit[1]!='INTO' or qsplit[3]!='VALUES':
        return 0

    table1 = qsplit[2]
    flag = checkTableColumnSyntax(table1)
    if flag==0:
        print(f"\n\n--Invalid syntax: Table: '{table1}' syntax not valid--")
        return 0


    # check VALUES Syntax
    value_split = qsplit[4:]
    print(f"DEBUG info--VALUES: '{value_split}'")
    if value_split[0][0]!='(' or value_split[-1][-1]!=')':
        print("\n\n--Invalid syntax: Values must be inside parentheis--")
        return 0

    # remove parenthesis from the VALUES split
    if value_split[0]=='(':
        value_split=value_split[1:]
    else:
        value_split[0]=value_split[0][1:]

    if value_split[-1]==')':
        value_split=value_split[:-1]
    else:
        value_split[-1]=value_split[-1][:-1]
    print(f"DEBUG info--VALUES without parenthesis: '{value_split}'")
    # miniDB call

    db.insert(table1, value_split)

    return 1
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
def DELETE(qsplit, db):
    if db==0:
        print("\n\n--LOAD A DATABASE--")
        return 0


    qsplit_len = len(qsplit)

    #  DELETE FROM table1 SET ... legal length = 5
    if qsplit_len!=5 or qsplit[1]!='FROM' or qsplit[3]!='WHERE':
        return 0

    table1 = qsplit[2]
    flag = checkTableColumnSyntax(table1)
    if flag==0:
        print(f"\n\n--Invalid syntax: Table: '{table1}' syntax not valid--")
        return 0


    condition = parseCondition(qsplit, 4, table1)
    if not condition:
        return 0


    db.delete(table1, condition)


#-----------------------END OF FUNCTIONS----------------------------------------
#-------------------------------------------------------------------------------
# SQL query start up


#db_load  Database('smdb', 1)
#-------------------------------------------------------------------------------
#------------------------------MAIN ROUTINE-------------------------------------
#-------------------------------------------------------------------------------
while True:
    # take user SQl query input string
    load = ""
    if db_load==0:
        load = "--LOAD a DATABASE--"
    try:
        query = input(f"\n--SQL Query (enter Q to quit) {load}\n\n>>> ")
    except KeyboardInterrupt:
        print('\nEnter Q to quit')
        continue
    except EOFError:
        print('Enter Q to quit')
        continue


    if query=='Q':  # Quit if Q is entered
        break

    q_split = query.split()  # split the given input string to a list of strings

    try:

        basicSyntaxFlag = basicSyntaxCheck(q_split)
        q_split = delQuestionMark(q_split)  # remove ';' from the very and of the query
        q_split = uppercaseSQLkeyWords(q_split)  # make SQL clauses case-insensitive
    except IndexError:
        print('Enter Q to quit')
        continue
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    '''
        ------------------------------------------------------------------------
        --------------------- LOAD DATABASE ------------------------------------
    '''
    if basicSyntaxFlag!=0 and q_split[0]=='LOAD':
        db_load = LOAD(q_split)
        if db_load== -1:
            print("\n\n--Invalid syntax: Not valid SQL query--")

        continue
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    #print(q_split)
    #---------------------------------------------------------------------------
    #----------------------SQL query syntax check-------------------------------
    if basicSyntaxFlag==0 or executeOperation(q_split, db_load)==0:
        print("\nERROR--Not valid SQL query, try again")
