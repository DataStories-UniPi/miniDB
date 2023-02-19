import mdb


def eqivalent_querys(query):
    #takes a query and return all the equivalent querys that miniDB support
    Eqivalent_querys = []
    if type(query['from']) == dict or len(query['where'].split('and')) >= 2:
        for item in suffle_all_combinations(analyse_multiple_select(query)[0]):
            for From in analyse_multiple_select(query)[1]:
                Eqivalent_querys.append(combine_multiple_selects(item,From).copy())
    return Eqivalent_querys

def alternative_join(query):# returnd the alternative join of a join query
    alt_query = query.copy()
    if query['join'] == 'right':
        alt_query['join'] = 'left'
    elif query['join'] == 'left':
        alt_query['join'] = 'right'
    alt_query['right'] = query['left']
    alt_query['left'] = query['right']
    return alt_query

def analyse_multiple_select(query):
    #takes a list of combined select querys split them and returns a splited list of select querys and the table of the query
    #for example takes a quwery of multiple selects ??1(??2(...(??n(instructor)))) and returns [??1,??2,...,??n],instructor
    #also if a condition has 'and' split the condition and turn the query into multiple selects 
    #for example takes ??1^?2^...^??(instructor) and returns [??1,??2,...,??n],instructor
    Query_list=[]
    From = []
    if len(query['where'].split('and')) >= 2:
        for item in query['where'].split('and'):
            Query_list.append({'select':query['select'],'from':'','where':item,'distinct':query['distinct'],'order by':query['order by'],'limit':query['limit'],'desc':query['desc']})
    else:
        Query_list.append({'select':query['select'],'from':'','where':query['where'],'distinct':query['distinct'],'order by':query['order by'],'limit':query['limit'],'desc':query['desc']})
    if type(query['from']) == dict:
        if query['from'].get('select') != None:
            for item in analyse_multiple_select(query['from'])[0]:
                Query_list.append(item)
            for item in analyse_multiple_select(query['from'])[1]:
                From.append(item)
        elif query['from'].get('join') != None:
            From.append(query['from'])
            From.append(alternative_join(query['from']))
    else:
        From.append(query['from'])
        
    return Query_list,From

def suffle_all_combinations(List):# return all shuffled combinations of a List
    temp_list = []
    if len(List) >= 2:
        for list_item in suffle_all_combinations(List[1:]).copy():
            for i in range(len(list_item)+1):
                tmp = list_item.copy()
                tmp.insert(i,List[0])
                temp_list.append(tmp)
    else:
         temp_list.append(List)
    return temp_list

def combine_multiple_selects(list_selects,_from):
    # takes a list of select querys and a table and creates a combined select query
    # for example takes the list [??1,??2,...,??n] and one or more tables for example 'instructor' and returns ??1(??2(...(??n(instructor))))
    list_selects[len(list_selects)-1]['from'] = _from
    for i in range(len(list_selects)-1,0,-1):
        list_selects[i-1]['from'] = list_selects[i].copy()
    return list_selects[0]


for item in eqivalent_querys(mdb.interpret('select * from table1  where id>17 and id<20')):
    print(item)



