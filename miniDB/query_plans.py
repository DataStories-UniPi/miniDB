import copy
def get_final_from(dic):
    if 'from' in dic:
        if isinstance(dic['from'],str):
            return dic['from']
        else:
            return get_final_from(dic['from'])
    else:
        return None

def multiple_query_plans(dic):
    

    Query_Plan_List=[]
    Query_Plan_List.append(dic)
    query_plan_1 = copy.deepcopy(dic)
    
    
    
    if query_plan_1['where'] and query_plan_1['from'] is None:
        return dic
    
    if isinstance(query_plan_1['from'],dict):
        # Check if the first and second rules of relational algebra (RA) can be applied to the query plan
        if 'select' in  query_plan_1['from'].keys() and query_plan_1['where'] != None:#if isinstance(query_plan_1['from'],dict) 'select' in  query_plan_1['from'].keys() and query_plan_1['where'] != None
            '''

            The first and second rules of RA are applied in order to optimize the query plan.
            The first step creates two separate query plans, each containing one of the two operands of the 'and' operator in the WHERE clause.
            The second step creates a new query plan by combining the two previous ones with a new 'and' operator.
            The third and final step updates the original query plan with the new values for 'from' and 'where', and adds it to the list of query plans to be executed.

            '''
            query_plan_2 = copy.deepcopy(query_plan_1) 
            query_plan_2['from'] = query_plan_2['from']['from']
            query_plan_2['where'] = {'and':{'left':query_plan_1['where'],'right':query_plan_1['from']['where']}}#first step
            Query_Plan_List.append(query_plan_2)
            query_plan_3 = copy.deepcopy(query_plan_2)
            query_plan_3['where'] = {'and':{'left':query_plan_1['from']['where'],'right':query_plan_1['where']}}#second step
            Query_Plan_List.append(query_plan_3)
            query_plan_4 = copy.deepcopy(query_plan_3)
            query_plan_1['where'] = query_plan_4['where']['and']['left']#third step
            query_plan_1['from'] = query_plan_4
            query_plan_1['from']['where'] = query_plan_4['where']['and']['right']
            Query_Plan_List.append(query_plan_1)


        # Apply the third rule of relational algebra (RA) to the query plan
        elif 'select' in query_plan_1['from'].keys() and query_plan_1['where'] == None:
            '''

            The third rule of RA is applied to the query plan when it only contains a SELECT clause and no WHERE clause.
            The 'get_final_from' function is called to recursively retrieve the final source relation(s) and replace them in the query plan.
            The updated query plan is then added to the list of query plans to be executed.

            '''
            query_plan_1['from'] = get_final_from(query_plan_1)
            Query_Plan_List.append(query_plan_1)

        # This block of code checks if the fourth and fifth rules of relational algebra can be applied to the given query plan.   
        # If so, it creates several new query plans and appends them to the Query_Plan_List for further evaluation.
        elif 'join' in query_plan_1['from'].keys() and query_plan_1['where'] != None:
            '''

            For queries that have a WHERE clause (query_plan_1), the following steps are taken:
            query_plan_2: Create a new query plan by copying the existing one and replacing the 'from' clause with the right operand of the 'on' clause.
            query_plan_1: Remove the 'where' clause and set it to None, and update the 'on' clause to its right operand.
            query_plan_3: Create a new query plan by copying the existing one and replacing the 'on' clause with a new 'and' clause that has the original 
            'on' clause on the left and the original 'where' clause on the right. Set the 'where' clause to None.
            query_plan_4: Create a new query plan by swapping the left and right operands of the 'from' clause.
            query_plan_5: Create a new query plan by swapping the left and right operands of the 'from' clause in the original query plan.
            query_plan_6: Create a new query plan by swapping the left and right operands of the 'from' clause in query_plan_3.

            All new query plans are added to the Query_Plan_List for further evaluation and optimization.

            '''
            on_clause = copy.deepcopy(query_plan_1['from']['on'])
            split_on_clause = on_clause.split("=")
            split_on_clause.insert(1, "=")
            split_on_clause[0], split_on_clause[2] = split_on_clause[2], split_on_clause[0]
            new_on_clause = "".join(split_on_clause)
            second_on_clause = copy.deepcopy(new_on_clause)
            query_plan_2 = copy.deepcopy(query_plan_1)
            query_plan_1['from']['on'] = {'and':{'left':query_plan_2['where'],'right':query_plan_2['from']['on']}}
            query_plan_1['where'] = None
            Query_Plan_List.append(query_plan_1)
            query_plan_3 = copy.deepcopy(query_plan_1)
            query_plan_3['from']['on']['and']['left'] = query_plan_1['from']['on']['and']['right']
            query_plan_3['from']['on']['and']['right'] = query_plan_1['from']['on']['and']['left']
            Query_Plan_List.append(query_plan_3)
            query_plan_4 = copy.deepcopy(dic)
            query_plan_4['from']['on'] = second_on_clause
            query_plan_4['from']['left'] = dic['from']['right']
            query_plan_4['from']['right'] = dic['from']['left']
            Query_Plan_List.append(query_plan_4)
            query_plan_5 = copy.deepcopy(query_plan_1)
            
            
            query_plan_5['from']['left'] = query_plan_1['from']['right']
            query_plan_5['from']['right'] = query_plan_1['from']['left']
            Query_Plan_List.append(query_plan_5)
            query_plan_6 = copy.deepcopy(query_plan_3)
            query_plan_6['from']['left'] = query_plan_3['from']['right']
            query_plan_6['from']['right'] = query_plan_3['from']['left']
            Query_Plan_List.append(query_plan_6)

        # This block of code checks if the fourth and fifth rules of relational algebra can be applied to the given query plan.   
        # If so, it creates several new query plans and appends them to the Query_Plan_List for further evaluation.
        elif 'join' in query_plan_1['from'].keys() and query_plan_1['where'] == None:
            '''

            For queries that do not have a WHERE clause (query_plan_1), the following steps are taken:
            query_plan_2: Create a new query plan by copying the existing one and replacing the 'from' clause with the right operand of the 'on' clause.
            query_plan_1: Update the 'on' clause to its right operand and set the 'where' clause to its left operand.
            query_plan_3: Create a new query plan by copying the existing one and replacing the 'on' clause with a new 'and' clause that has the original 
            'on' clause on the left and the original 'where' clause on the right. Set the 'where' clause to None.
            query_plan_4: Create a new query plan by swapping the left and right operands of the 'from' clause.
            query_plan_5: Create a new query plan by swapping the left and right operands of the 'from' clause in the original query plan.
            query_plan_6: Create a new query plan by swapping the left and right operands of the 'from' clause in query_plan_3.

            All new query plans are added to the Query_Plan_List for further evaluation and optimization.

            '''
            
            query_plan_2 = copy.deepcopy(query_plan_1)
            query_plan_1['from']['on'] = query_plan_2['from']['on']['and']['right']
            query_plan_1['where'] = query_plan_2['from']['on']['and']['left']
            Query_Plan_List.append(query_plan_1)
            on_clause = copy.deepcopy(query_plan_1['from']['on'])
            split_on_clause = on_clause.split("=")
            split_on_clause.insert(1, "=")
            split_on_clause[0], split_on_clause[2] = split_on_clause[2], split_on_clause[0]
            new_on_clause = "".join(split_on_clause)
            second_on_clause = copy.deepcopy(new_on_clause)
            query_plan_3 = copy.deepcopy(query_plan_1)
            query_plan_3['from']['on'] = {'and':{'left':query_plan_1['from']['on'],'right':query_plan_1['where']}}
            query_plan_3['where'] = None
            Query_Plan_List.append(query_plan_3)
            query_plan_4 = copy.deepcopy(dic)
            query_plan_4['from']['left'] = dic['from']['right']
            query_plan_4['from']['right'] = dic['from']['left']
            Query_Plan_List.append(query_plan_4)
            query_plan_5 = copy.deepcopy(query_plan_1)
            query_plan_5['from']['on'] = second_on_clause
            query_plan_5['from']['left'] = query_plan_1['from']['right']
            query_plan_5['from']['right'] = query_plan_1['from']['left']
            Query_Plan_List.append(query_plan_5)
            query_plan_6 = copy.deepcopy(query_plan_3)
            query_plan_6['from']['left'] = query_plan_3['from']['right']
            query_plan_6['from']['right'] = query_plan_3['from']['left']
            Query_Plan_List.append(query_plan_6)

        # This block of code applies the fifth rule of relational algebra to the query plan.    
        elif ('join' in query_plan_1['from'].keys() and query_plan_1['where'] == None) and isinstance(query_plan_1['from']['on'],str): 
            '''

            If the query involves a join and doesn't have a WHERE clause, the left and right operands of the join are swapped.
            A new query plan is created by copying the original query plan and modifying its 'from' clause, and then it is added to Query_Plan_List for further evaluation and optimization.
                        
            '''
            on_clause = copy.deepcopy(query_plan_1['from']['on'])
            split_on_clause = on_clause.split("=")
            split_on_clause.insert(1, "=")
            split_on_clause[0], split_on_clause[2] = split_on_clause[2], split_on_clause[0]
            new_on_clause = "".join(split_on_clause)
            second_on_clause = copy.deepcopy(new_on_clause)
            query_plan_2 = copy.deepcopy(query_plan_1)
            query_plan_1['from']['on'] = second_on_clause
            query_plan_1['from']['left'] = query_plan_2['from']['right']
            query_plan_1['from']['right'] = query_plan_2['from']['left']
            Query_Plan_List.append(query_plan_1)
    else:
        if isinstance(query_plan_1['from'], str) and query_plan_1['where'] == None:
            '''

            The third rule of RA is applied to the query plan when it only contains a SELECT clause and no WHERE clause.
            The 'get_final_from' function is called to recursively retrieve the final source relation(s) and replace them in the query plan.
            The updated query plan is then added to the list of query plans to be executed.

            '''
            query_plan_1['from'] = get_final_from(query_plan_1)
            Query_Plan_List.append(query_plan_1)
        elif isinstance(query_plan_1['where'],dict):
            #This block of code checks if the first and second rules of relational algebra can be applied to the given query plan.
            and_idx = [i for i,word in enumerate(query_plan_1['where'].keys()) if word=='and']
            '''

            If so, it creates several new query plans and appends them to the Query_Plan_List for further evaluation.
            'and' is used in the WHERE clause of the query_plan_1, therefore, this block of code checks if the WHERE clause contains 'and'
            If 'and' is found in the WHERE clause, create new query plans by applying the first and second rules of RA
                
            All new query plans are added to the Query_Plan_List for further evaluation and optimization.

            '''
            if and_idx :
                query_plan_2 = copy.deepcopy(query_plan_1)
                query_plan_1['where'] = query_plan_2['where']['and']['left']
                query_plan_1['from'] = query_plan_2
                query_plan_1['from']['where'] = query_plan_2['where']['and']['right']
                Query_Plan_List.append(query_plan_1)
                query_plan_3 = copy.deepcopy(query_plan_1)
                query_plan_3['where'] = query_plan_1['from']['where']
                query_plan_3['from']['where'] = query_plan_1['where']
                Query_Plan_List.append(query_plan_3)
                query_plan_4 = copy.deepcopy(query_plan_3)
                query_plan_4['from'] = query_plan_4['from']['from']
                query_plan_4['where'] = {'and':{'left':query_plan_3['where'],'right':query_plan_3['from']['where']}}
                Query_Plan_List.append(query_plan_4)
               
    unique_dictionaries = []
    for dictionary in Query_Plan_List:
        if dictionary not in unique_dictionaries:
            
            unique_dictionaries.append(dictionary)
    
    for index, dictionary in enumerate(unique_dictionaries):
        print(f"Dictionary {index + 1}:")
        print(dictionary)
    return unique_dictionaries
    

    
        
