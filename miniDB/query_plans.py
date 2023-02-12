import copy
def get_final_from(dic):
    if 'from' in dic:
        if type(dic['from']) == str:
            return dic['from']
        else:
            return get_final_from(dic['from'])
    else:
        return None

def sqlify(query):
    sql = "SELECT "
    
    if query.get("distinct"):
        sql += "DISTINCT "
    
    select = query.get("select")
    if type(select) == str:
        sql += select + " "
    elif type(select) == dict:
        nested_select_sql, _ = sqlify(select)
        sql += nested_select_sql + " "
    
    from_clause = query.get("from")
    if type(from_clause) == str:
        sql += "FROM " + from_clause + " "
    elif type(from_clause) == dict:
        if from_clause.get("join"):
            sql += "FROM " + from_clause.get("left") + " "
            sql += from_clause.get("join").upper() + " JOIN "
            sql += from_clause.get("right") + " "
            on_clause = from_clause.get("on")
            if type(on_clause) == str:
                sql += "ON " + on_clause + " "
            elif type(on_clause) == dict:
                if on_clause.get("and"):
                    sql += "ON " + on_clause.get("left") + " AND " + on_clause.get("right") + " "
    
    where = query.get("where")
    if where:
        if type(where) == str:
            sql += "WHERE " + where + " "
        elif type(where) == dict:
            if where.get("and"):
                sql += "WHERE " + where.get("left") + " AND " + where.get("right") + " "
    
    order_by = query.get("order by")
    if order_by:
        sql += "ORDER BY " + order_by + " "
        if query.get("desc"):
            sql += "DESC "
    
    limit = query.get("limit")
    if limit:
        sql += "LIMIT " + str(limit)
    
    return sql


def multiple_query_plans(dic):
    
    Query_Plan_List=[]
    Query_Plan_List.append(dic)
    query_plan = copy.deepcopy(dic)
    dict = copy.deepcopy(query_plan)
    
    
    if query_plan['where'] and query_plan['from'] is None:
        return dic
    
    try:
        if 'select' in  query_plan['from'].keys() and query_plan['where'] != None:#first and second rule of RA
            
            dict['from'] = dict['from']['from']
            dict['where'] = {'and':{'left':query_plan['where'],'right':query_plan['from']['where']}}
            Query_Plan_List.append(dict)
            tempo = copy.deepcopy(dict)
            tempo['where'] = {'and':{'left':query_plan['from']['where'],'right':query_plan['where']}}
            Query_Plan_List.append(tempo)
            tempo2 = copy.deepcopy(tempo)
            query_plan['where'] = tempo2['where']['and']['left']
            query_plan['from'] = tempo2
            query_plan['from']['where'] = tempo2['where']['and']['right']
            Query_Plan_List.append(query_plan)
            '''
        elif 'join' in query_plan['from'].keys() and 'join' in query_plan['from']['left']['from']:#6th rule 
            
            query_plan['from']['left'] = dict['from']['left']['from']['left']
            query_plan['from']['right'] = dict['from']['left']
            query_plan['from']['right']['from']['left'] = dict['from']['left']['from']['right']
            query_plan['from']['right']['from']['right'] = dict['from']['right']
            query_plan['from']['right']['from']['on'] = query_plan['from']['on']
            query_plan['from']['on'] = dict['from']['left']['from']['on']
            Query_Plan_List.append(query_plan)
            '''
        elif ('select' in query_plan['from'].keys() and query_plan['where'] == None) or (isinstance(query_plan['from'], str) and query_plan['where'] == None): #third rule of RA
            query_plan['from'] = get_final_from(query_plan)
            Query_Plan_List.append(query_plan)
        elif 'join' in query_plan['from'].keys() and query_plan['where'] != None:#4th and 5th rule of RA
            query_plan['from']['on'] = {'and':{'left':dict['where'],'right':dict['from']['on']}}
            query_plan['where'] = None
            Query_Plan_List.append(query_plan)
            tempo = copy.deepcopy(query_plan)
            tempo['from']['on']['and']['left'] = query_plan['from']['on']['and']['right']
            tempo['from']['on']['and']['right'] = query_plan['from']['on']['and']['left']
            Query_Plan_List.append(tempo)
            tempo2 = copy.deepcopy(tempo)
            tempo2['from']['on'] = tempo['from']['on']['and']['right']
            tempo2['where'] = tempo['from']['on']['and']['left']
            Query_Plan_List.append(tempo2)
            tempo3 = copy.deepcopy(dic)
            tempo3['from']['left'] = dic['from']['right']
            tempo3['from']['right'] = dic['from']['left']
            Query_Plan_List.append(tempo3)
            tempo4 = copy.deepcopy(query_plan)
            tempo4['from']['left'] = query_plan['from']['right']
            tempo4['from']['right'] = query_plan['from']['left']
            Query_Plan_List.append(tempo4)
            tempo5 = copy.deepcopy(tempo)
            tempo5['from']['left'] = tempo['from']['right']
            tempo5['from']['right'] = tempo['from']['left']
            Query_Plan_List.append(tempo5)
            tempo6 = copy.deepcopy(tempo2)
            tempo6['from']['left'] = tempo2['from']['right']
            tempo6['from']['right'] = tempo2['from']['left']
            Query_Plan_List.append(tempo6)
        elif 'join' in query_plan['from'].keys() and query_plan['where'] == None:#4th and 5th rule of RA
            query_plan['from']['on'] = dict['from']['on']['and']['right']
            query_plan['where'] = dict['from']['on']['and']['left']
            Query_Plan_List.append(query_plan)
            tempo = copy.deepcopy(query_plan)
            tempo['from']['on'] = dict['from']['on']['and']['left']
            tempo['where'] = dict['from']['on']['and']['right']
            Query_Plan_List.append(tempo)
            tempo2 = copy.deepcopy(tempo)
            tempo2['from']['on'] = {'and':{'left':tempo['where'],'right':tempo['from']['on']}}
            tempo2['where'] = None
            Query_Plan_List.append(tempo2)
            tempo3 = copy.deepcopy(dic)
            tempo3['from']['left'] = dic['from']['right']
            tempo3['from']['right'] = dic['from']['left']
            Query_Plan_List.append(tempo3)
            tempo4 = copy.deepcopy(query_plan)
            tempo4['from']['left'] = query_plan['from']['right']
            tempo4['from']['right'] = query_plan['from']['left']
            Query_Plan_List.append(tempo4)
            tempo5 = copy.deepcopy(tempo)
            tempo5['from']['left'] = tempo['from']['right']
            tempo5['from']['right'] = tempo['from']['left']
            Query_Plan_List.append(tempo5)
            tempo6 = copy.deepcopy(tempo2)
            tempo6['from']['left'] = tempo2['from']['right']
            tempo6['from']['right'] = tempo2['from']['left']
            Query_Plan_List.append(tempo6)
    except:
            try:
                and_idx = [i for i,word in enumerate(query_plan['where'].keys()) if word=='and']#first and second rule of RA
                if and_idx :
                    query_plan['where'] = dict['where']['and']['left']
                    query_plan['from'] = dict
                    query_plan['from']['where'] = dict['where']['and']['right']
                    Query_Plan_List.append(query_plan)
                    tempo = copy.deepcopy(query_plan)
                    tempo['where'] = query_plan['from']['where']
                    tempo['from']['where'] = query_plan['where']
                    Query_Plan_List.append(tempo)
                    tempo2 = copy.deepcopy(tempo)
                    tempo2['from'] = tempo2['from']['from']
                    tempo2['where'] = {'and':{'left':tempo['where'],'right':tempo['from']['where']}}
                    Query_Plan_List.append(tempo2)
            except:
                try:
                    if 'join' in query_plan['from'].keys() and query_plan['where'] == None:#fifth rule of RA
                        query_plan['from']['left'] = dict['from']['right']
                        query_plan['from']['right'] = dict['from']['left']
                        Query_Plan_List.append(query_plan)
                except:
                    pass       
    unique_dictionaries = []
    for dictionary in Query_Plan_List:
        if dictionary not in unique_dictionaries:
            
            unique_dictionaries.append(dictionary)
    print(unique_dictionaries)
    #for index, dictionary in enumerate(unique_dictionaries):
        #print(f"Dictionary {index + 1}:")
        #print(dictionary)
    return unique_dictionaries
    

    
        
