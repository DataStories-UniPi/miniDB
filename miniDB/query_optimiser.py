'''
INFO
input : sql query
output relational algebra expression
'''
sql_input = '''SELECT *
FROM instructor AS I
INNER JOIN teaches AS T ON I.ID = T.ID
WHERE I.dept_name = “Music” AND T.year = 2009 '''

def sql_to_rel(query):
    query_words = query.split()
    dict={
        "SELECT":"σ",
        "*":"",
        "FROM":"",
        "INNER JOIN":"⋈",
        "ON":"",
        "WHERE":""
    }
    ra=[]
    flag_table = False
    flag_condition = 0
    tables=[]
    join =""
    conditions=[]
    for word in query_words:
        if word == "SELECT":
            ra.append("σ")
        elif word == "AS":
            flag_table=True
        elif flag_table:
            tables.append(word)
            flag_table=False
        elif word=="ON":
            flag_condition=5
        elif flag_condition>0:
            conditions.append(word)
            flag_condition--1
        elif word == "JOIN":
           join = "⋈"


    for c in conditions:
        if c == "AND":
            ra.append("^")
            continue
        elif c == "WHERE":
            continue
        ra.append(c)
    for t in tables:
        ra.append(t)
        ra.append(join)
        join = ""
    print("Input sql -> ",sql_input)
    print("RA -> ",ra)

sql_to_rel(sql_input)