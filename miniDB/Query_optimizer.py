'''
In this file, there are all the methods needed for query optimization
'''
from misc import split_complex_conditions,split_condition,return_tables_in_join_dict


def build_equivalent_query(originalQuery,db):
    '''
    Input: Gets the original query interpreted as well as 
    Output: produces equivelant queries
    '''

    Equivalent_queries = [] 

    notCheckedQueries = [originalQuery] # queries not checked for equivalent ones

    for i in notCheckedQueries: # for every new query to check

        tempNewQueries = [i]


        # make equivalent based on subqueries
        for j in i.keys(): 
            if isinstance(i[j],dict): # for each subquery

                equivalent_subQueries = build_equivalent_query(i[j],db) # find equivalent subqueries

                # combine subquery with the rest of the query and add to the temp list 
                for k in equivalent_subQueries:
                    t = i
                    i[j] = k
                    if t not in Equivalent_queries and t not in tempNewQueries:
                        tempNewQueries.append(t)


        # make equivalent subqueries based on query plan i and add them to list r
        r = rule1(i)              
        r.append(rule2(i)) 
        r.append(rule3(i)) 
        r.append(rule4(i)) 
        r.append(rule5(i))  
        r.append(rule7(i,db)) 
        r.append(rule8(i,db)) 


        for t in r:
            if t not in Equivalent_queries and t not in tempNewQueries:
                tempNewQueries.append(t) # add all subqueries to the list of produced ones

        # Query plan i has been precessed
        # move it to Equivalent_queries list
        Equivalent_queries.append(i)
        notCheckedQueries.remove(i)

        # add the produced query plans to the notCheckedQueries
        for t in tempNewQueries:
            if t not in Equivalent_queries and t not in tempNewQueries:
                notCheckedQueries.append(t)

    return Equivalent_queries
        


    


def rule1(queryPlan):
    '''
    This function checks for the 1st RA RULE for building equivelant query
    σθ1∧θ2 (E) = σθ1 (σθ2 (E))
    '''

    EquivalentQuery = []

    newTempQuery = {}

    # combining 2 nested selections into 1 
    if 'select' in queryPlan.keys() and isinstance(queryPlan['from'],dict) and 'select' in queryPlan['from'].keys(): # if there is nested selection
        if not (queryPlan['distinct']==False and queryPlan['from']['distinct']==True and queryPlan['where']!=queryPlan['from']['where']):
            # if the selection inside 'from' is distinct but the selection outside isn't, we cant combine the 2 selections in one if 'select' is differrent
            
            # the 2 selection statements can be combined, combine them
            newTempQuery = queryPlan

            if (newTempQuery['from'])['where'] != None:
                newTempQuery['where'] += ' and ' + newTempQuery['from']['where']
            if (newTempQuery['order by'] == None):
                newTempQuery['order by'] = queryPlan['from']['order by']
            if (newTempQuery['desc'] == None):
                newTempQuery['desc'] = (queryPlan['from'])['desc']
            if newTempQuery['limit']!=None or (queryPlan['from'])['limit']!=None:
                newTempQuery['limit'] = min(i for i in [newTempQuery['limit'] , queryPlan['from']['limit']] if i is not None  )

            newTempQuery['from'] = newTempQuery['from']['from']

            EquivalentQuery.append(newTempQuery) # add equivalent query to list

        
    if 'select' in queryPlan.keys() and 'and' in split_complex_conditions(queryPlan['where']):
        # if there is plan for select and where has conditions connected with 'and'

        '''
        check for high priority
        '''
        # if we can seperate them
        split_condition = split_complex_conditions(queryPlan['where'])

        # seperate into 2
        if split_condition[-2] == 'and':
            newTempQuery = {
                'select' : queryPlan['select'],
                'from': queryPlan['from'],            
                'order by' : queryPlan['order by'],
                'desc' : queryPlan['desc'],
                'limit' : queryPlan['limit'],
                'where': split_condition[0:-2]
            }

            newTempQuery = {
                'select' : queryPlan['select'],
                'from': newTempQuery,            
                'order by' : queryPlan['order by'],
                'desc' : queryPlan['desc'],
                'limit' : queryPlan['limit'],
                'where': split_condition[-1]
            }

            EquivalentQuery.append(newTempQuery)

    return EquivalentQuery

def rule2(queryPlan): 
    '''
    This function checks for the 2nd RA RULE for building equivelant query
    σθ1 (σθ2 (E)) = σθ2 (σθ1 (E))
    '''
    # only 1 equivelant query can be built from this rule
    newTempQuery = {}

    if 'select' in queryPlan.keys() and isinstance(queryPlan['from'],dict) and 'select' in queryPlan['from'].keys(): # if there is nested selection
        if not (queryPlan['distinct']==False and queryPlan['from']['distinct']==True and queryPlan['where']!=queryPlan['from']['where']):
            # if the selection inside 'from' is distinct but the selection outside isn't, we cant combine the 2 selections in one if 'where' is differrent
            if queryPlan['limit'] == None and queryPlan['from']['limit'] == None:
                # if there is limit we won't get the same result. We need to make sure there is no limit
            
                newTempQuery = queryPlan
                newTempQuery['where'] = newTempQuery['from']['where']
                newTempQuery['from']['where'] = queryPlan['where']
                return [newTempQuery]

    return []

def rule3(queryPlan):
    '''
    This function checks for the 3rd RA RULE for building equivelant query
    ΠL1 (ΠL2 (. . .(ΠLn (E)). . .)) = ΠL1 (E)
    '''
    # only 1 equivelant query plan can be returned
    newTempQuery = {}

    
    if 'select' in queryPlan.keys() and isinstance(queryPlan['from'],dict) and 'select' in queryPlan['from'].keys(): # if there is nested selection
        if queryPlan['where'] == None: # if one of the selections doesn't have condition
            # we can combine them

            newTempQuery = queryPlan

            if queryPlan['select'] != '*':
                newTempQuery['select'] = queryPlan['select']
            else:
                newTempQuery['select'] = queryPlan['from']['select']

            newTempQuery['from'] = queryPlan['from']['from']

            if queryPlan['from']['distinct'] == True:
                newTempQuery['distinct'] = True                      

            if newTempQuery['order by'] == None:
                newTempQuery['order by'] = queryPlan['from']['order by']

            if (newTempQuery['desc'] == None):
                newTempQuery['desc'] = (queryPlan['from'])['desc']

            if newTempQuery['limit']!=None or (queryPlan['from'])['limit']!=None:
                newTempQuery['limit'] = min(i for i in [newTempQuery['limit'] , queryPlan['from']['limit']] if i is not None  )

            if queryPlan['from']['where'] != None:
                newTempQuery['where'] = queryPlan['from']['where']

            return [newTempQuery]
        
    return []

def rule4(queryPlan):
    '''
    This function checks for the 4th RA RULE for building equivelant query
    σθ1 (E1   ⊲⊳θ2   E2) = E1   ⊲⊳θ1∧θ2   E2
    '''
    newTempQuery = {}

    if 'select' in queryPlan.keys() and isinstance(queryPlan['from'],dict) and 'join' in queryPlan['from'].keys(): # if there is join statement inside a select statement
        if queryPlan['select']=='*' and queryPlan['distinct']==None and queryPlan['order by']==None and queryPlan['limit']==None and queryPlan['desc']==None:
            # if select statement is only used for the condition

            newTempQuery = queryPlan['from']
            if queryPlan['where'] is not None:
                newTempQuery['on'] += ' and ' + queryPlan['where']
            return [newTempQuery]
    # based on the miniDB manual, user is not supposed to use join on its own as command. They should use join command inside select command
    # so we don't need to check for the rule reversed.
    return []

def rule5(queryPlan):
    '''
    This function checks for the 5th RA RULE for building equivelant query
    E1 ⊲⊳θ E2 = E2 ⊲⊳θ E1
    '''
    
    if 'join' in queryPlan.keys():

        newTempQuery = queryPlan
        newTempQuery['left'] = newTempQuery['right']
        newTempQuery['right'] = queryPlan['left']
        return [newTempQuery]
    
    return []
        
def rule7(queryPlan,db):
    '''
    This function checks for the 7th RA RULE for building equivelant query
    It needs to check if a column inside a condition is in the left or right table in join so we pass a database object 
    σθ1∧θ2 (E1 ⊲⊳θ E2) = (σθ1 (E1)) ⊲⊳θ (σθ2 (E2))
    '''        
    EquivalentQueries = []

    if 'select' in queryPlan.keys():
            if (isinstance(queryPlan['from'], dict)) and ('join' in queryPlan['from'].keys()): # if selection is done on 2 joined tables
                # we can do the selection before join

                if queryPlan['where'] != None: # if selection is performed based on condition

                    broken_complex_condition = split_complex_conditions(queryPlan['where']) # we break the condition of the selection
                    
                    if 'or' not in broken_complex_condition: # conditions have to be connected with 'and'
                        # we'll make a list with the conditions on the left and one on the right table                                

                        # get the names of the tables joined
                        Left_table, Right_table = return_tables_in_join_dict(queryPlan['from'])

                        Left_Table_Columns = db.return_table_column_names(Left_table) # all columns that come from tables in the left of the join
                        Right_Table_Columns  = db.return_table_column_names(Right_table) # all columns that come from tables in the right of the join

                        Left_Table_Conditions = [] # conditions for left table
                        Right_Table_Conditions = [] # conditions for right table

                        for x in broken_complex_condition:
                            if x != 'and': # for each condition 
                                if split_condition(x)[0] in Left_Table_Columns:
                                    Left_Table_Conditions.append(x)
                                elif split_condition(x)[0] in Right_Table_Columns:
                                    Right_Table_Conditions.append(x)                    


                        # find the selection queries dict for each of the tables (left and right)
                        left_table_selection_dict = {'select': '*',
                                            'from': queryPlan['from']['left'],
                                            'where': (' '.join(str(x)+' and ' for x in Left_Table_Conditions)).removesuffix(' and '),
                                            'distinct': None,
                                            'order by': None,
                                            'limit': None,
                                            'desc': None}
                        
                        right_table_selection_dict = {'select': '*',
                                            'from': queryPlan['from']['right'],
                                            'where': (' '.join(str(x)+' and ' for x in Right_Table_Conditions)).removesuffix(' and '),
                                            'distinct': None,
                                            'order by': None,
                                            'limit': None,
                                            'desc': None}
                        
                        # Ready to make the new query
                        newTempQuery = queryPlan # new query needs to have the selection
                        newTempQuery['where'] = None # however the where statement will not be check.
                        newTempQuery['from']['left'] = left_table_selection_dict # replace the left table with the selection to reduce rows
                        newTempQuery['from']['right'] = right_table_selection_dict # replace the left table with the selection to reduce rows

                        # check for equivalent queries in each sub-table from join to improve query
                        newTempQuery['from']['left'] = build_equivalent_query(newTempQuery['from']['left'])
                        newTempQuery['from']['right'] = build_equivalent_query(newTempQuery['from']['right'])

                        EquivalentQueries.append(newTempQuery)

                        return [newTempQuery]
                
def rule8(queryPlan,db):
    '''
    This function checks for the 8th RA RULE for building equivelant query
    It needs to check if a column inside a condition is in the left or right table in join so we pass a database object 
    a)	ΠL1∪L2 (E1 ⊲⊳θ E2) = (ΠL1 (E1)) ⊲⊳θ (ΠL2 (E2)) 
    b)	ΠL1∪L2 (E1 ⊲⊳θ E2) = ΠL1∪L2 ((ΠL1∪L3 (E1)) ⊲⊳θ (ΠL2∪L4 (E2)))
    '''        

    if 'select' in queryPlan.keys() and queryPlan['where']: # there is selection with no condition.
        if (isinstance(queryPlan['from'], dict)) and ('join' in queryPlan['from'].keys()): # if selection is done on 2 joined tables
            
            # Find columns that are present in select
            selected_columns = queryPlan['select']
            if selected_columns == '*':
                return            
            selected_columns = selected_columns.split(',')

            # Find columns inside the join condition 
            columns_in_join_condition = []
            broken_complex_condition = split_complex_conditions(queryPlan['where'])
            for i in broken_complex_condition:
                columns_in_join_condition.append(split_condition(i)[0])

            
            is_b_rule = False

            # for b rule 
            if not all(item in columns_in_join_condition for item in selected_columns):
                is_b_rule = True
                # keep only columns not present in selected_columns in columns_in_join_condition 
                for i in columns_in_join_condition:
                    if i in selected_columns: 
                        columns_in_join_condition.remove(i)
                
                # pretend they are selected 
                for i in columns_in_join_condition:
                    selected_columns.append(i)

                # follow same procedure as rule a            
            
            newTempQuery = queryPlan

            # find the selection queries dict for each of the joined tables (left and right)
            left_table_selection_dict = {'select': '',
                                        'from': queryPlan['from']['left'],
                                        'where': None,
                                        'distinct': None,
                                        'order by': None,
                                        'limit': None,
                                        'desc': None}
            
            right_table_selection_dict = {'select': '',
                                'from': queryPlan['from']['right'],
                                'where': None,
                                'distinct': None,
                                'order by': None,
                                'limit': None,
                                'desc': None}
            
            # finding columns that belong to the left and right table
            left,right = return_tables_in_join_dict(queryPlan['from']) # finding tables that take part in join
            left_columns = db.return_table_column_names(left)
            right_columns = db.return_table_column_names(right)

            # checking every column present in select statement
            for i in selected_columns:
                if i in left_columns: # if this column is from left table, add it to the select statement inside
                    left_table_selection_dict['select'] += i + ',' 
                elif i in right_columns: # else add it to right table dict
                    right_table_selection_dict['select'] += i + ','

            left_table_selection_dict['select'] = left_table_selection_dict['select'][:-1]
            right_table_selection_dict['select'] = right_table_selection_dict['select'][:-1]

            newTempQuery['from']['left'] = left_table_selection_dict
            newTempQuery['from']['right'] = right_table_selection_dict

            if is_b_rule: # add a select statement to keep only the columns needed for b rule

                newTempQuery = {'select': queryPlan['select'],
                                'from': newTempQuery,
                                'where': None,
                                'distinct': None,
                                'order by': None,
                                'limit': None,
                                'desc': None}

            return [newTempQuery]
        
    return []
                



            







