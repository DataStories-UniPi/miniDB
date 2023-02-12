import copy
def multiple_query_plans(dic):
    
    Query_Plan_List=[]
    Query_Plan_List.append(dic)
    query_plan = copy.deepcopy(dic)
    dict = query_plan.copy()
    

    #query_plan = {k: v for k, v in query_plan.items() if v is not None} Φτιάχνω ξανά το query_plan μόνο με τα key-value pairs που δεν έχουν None

    
    if query_plan['where'] and query_plan['from'] is None:
        return dic

    
    try:
        if 'select' in  Query_Plan_List[0]['from'].keys():
            tempo = Query_Plan_List[0].copy()
            tempo['from'] = tempo['from']['from']
            tempo['where'] = {'and':{'left':query_plan['where'],'right':query_plan['from']['where']}}
            Query_Plan_List.append(tempo)
            tempo2 = tempo.copy()
            tempo2['where'] = {'and':{'left':query_plan['from']['where'],'right':query_plan['where']}}
            Query_Plan_List.append(tempo2)
            tempo3 = tempo2.copy()
            dict['where'] = tempo3['where']['and']['left']
            dict['from'] = tempo3
            dict['from']['where'] = tempo3['where']['and']['right']
            Query_Plan_List.append(dict)

    except:
            try:
                and_idx = [i for i,word in enumerate(query_plan['where'].keys()) if word=='and']  
                if and_idx :
                    tempo = Query_Plan_List[0].copy()
                    tempo['where'] = query_plan['where']['and']['left']
                    tempo['from'] = query_plan
                    tempo['from']['where'] = query_plan['where']['and']['right']
                    Query_Plan_List.append(tempo)
                    tempo2 = tempo.copy() 
                    tempo3 = tempo2.copy()
                    dict['where'] = tempo2['from']['where']#query_plan['where']['and']['right']
                    dict['from'] = tempo2['from']
                    dict['from']['where'] = tempo3['where']#query_plan['where']['and']['left']
                    Query_Plan_List.append(dict)
                    tempo4 = tempo3.copy()
                    tempo4['from'] = tempo4['from']['from']
                    tempo4['where'] = {'and':{'left':tempo3['from']['where'],'right':tempo3['where']}}
                    Query_Plan_List.append(tempo3)
            except:
                print("something went wrong!!")
            
                 
        
    unique_dictionaries = []
    for dictionary in Query_Plan_List:
        if dictionary not in unique_dictionaries:
            unique_dictionaries.append(dictionary)



    for index, dictionary in enumerate(unique_dictionaries):
        print(f"Dictionary {index + 1}:")
        print(dictionary)
    

    
        
