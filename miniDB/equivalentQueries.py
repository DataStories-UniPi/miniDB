
import copy


def rule1(query):
    '''
    Given a query, apply the transformation rule
        σ q1∧q2 (R) = σ q1(σ q2 (R))
    query: the query to transform
    '''

    if query['where'] is not None and query['where'].find('and') != -1:
        # Split the query into two parts
        condition1, condition2 = query['where'].split('and')
        # Apply the rule
        equiv_query = copy.deepcopy(query) 
        equiv_query['where'] = condition1
        equiv_query['from'] = {'select': '*', 'from': equiv_query['from'], 'where': condition2,'distinct': None, 'orderby': None,'limit': None,'desc': None}
        return equiv_query
    return None
    
def rule2(query): 
    '''
    Rule 2: σθ1(σθ2(R)) = σθ2(σθ1(R))
    '''
    if 'select' in query.keys() and 'select' in query['from'].keys(): #and isinstance(query['from'],dict)
        condition1=query['where']  # θ1
        condition2=query['from']['where'] # θ2
        equiv_query = copy.deepcopy(query)
        equiv_query['where'] = condition2
        equiv_query['from']['where'] = condition1 
        return equiv_query
    return None

def rule3(query):
    '''
    Rule 3b: σθ1(R ⋈θ2 S) = R ⋈(θ1^θ2) S 
    '''
    if 'select' in query.keys() and isinstance(query['from'], dict) and query['from'].get('join') is not None:
        condition1=query['where']  # θ1
        condition2=query['from']['on'] # θ2
        equiv_query = copy.deepcopy(query) 
        equiv_query['where'] = None
        equiv_query['from']['on'] = condition1 + ' and ' + condition2 # θ1^θ2 as the join condition
        return equiv_query
    return None

def rule4(query):
    '''
    Given a query, apply the transformation rule
        R ⋈q S = S ⋈q R
    '''
    
    if 'select' in query.keys() and isinstance(query['from'], dict) and query['from'].get('join') is not None:
        left=query['from']['left']
        right=query['from']['right']
        equiv_query = copy.deepcopy(query)
        equiv_query['from']['left'] = right
        equiv_query['from']['right'] = left
        return equiv_query
    return None
    
def rule5(query):
    '''
    Given a query, apply the transformation rules:
       -( θ-join )
        (R ⋈q1 S) ⋈ q2^q3 T = R ⋈ q1^q3 (S ⋈ q2 T)
    '''
    # multiple joins are not supported by minidb for now
    '''
    if 'select' in query.keys() and query['from'].find('join') != -1 and query['where'] is not None: 
        condition=query['where']
        if condition.find('and') != -1:
            condition1=query['join']['on']
            condition2, condition3 = condition.split('and')
            # Apply the rule
            equiv_query = copy.deepcopy(query)
        return query
    return None
    '''
    pass
    


def rule6(query):
    '''
    Given a query, apply the transformation rules
        case 1: σ q1(R ⋈q S) = σ q1(R) ⋈q S 
        case 2: σ q1(R ⋈q S) = R ⋈q σ q1(S)
        case 3: σ q1^q2 (R ⋈q S) = σ q1(R) ⋈q σ q2 (S)
    '''
    equivalent_queries = []
    if 'select' in query.keys() and isinstance(query['from'], dict) and query['from'].get('join') is not None:
        if query['where'].find('and') != -1:
            # Split the condition into two parts
            condition1, condition2 = query['where'].split('and')
            # Apply the rule(case of double select condition: condition1 corresponds to left table and condition2 corresponds to right table)
            subquery1 = {'select': '*', 'from': query['from']['left'], 'where': condition1, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            subquery2 = {'select': '*', 'from': query['from']['right'], 'where': condition2, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            equiv_query= copy.deepcopy(query)
            equiv_query['from']['left']=subquery1
            equivalent_queries['from']['right']=subquery2
            equivalent_queries.append(equiv_query)
        
        # Apply the rule(case: condition corresponds only to left table)
        subquery1 = {'select': '*', 'from': query['from']['left'], 'where': query['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
        equiv_query= copy.deepcopy(query)
        equiv_query['from']['left']=subquery1
        equivalent_queries.append(equiv_query)
        # Apply the rule(case: condition corresponds only to right table)
        subquery2 = {'select': '*', 'from': query['from']['right'], 'where': query['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
        equiv_query= copy.deepcopy(query)
        equiv_query['from']['right']=subquery2
        equivalent_queries.append(equiv_query)
    return equivalent_queries
    
    
equiv_queries=[]
lastrule = None
def equiv_recursive(query):
    global lastrule # the last rule applied
    for rule in [rule1, rule2, rule3, rule4, rule5, rule6]:
        equiv_query = rule(query) # apply the rule
        if equiv_query is not None:
            lastrule = rule
            equiv_queries.append(equiv_query) # add the equivalent query to the list


# Example dictionary
exampleQuery = {'select': '*', 'from': {'select': '*', 'from': 'R', 'where': 'a > 5', 'distinct': None, 'orderby': None,'limit': None,'desc': None}, 'where': 'b < 10', 'distinct': None, 'orderby': None,'limit': None,'desc': None}
equiv_recursive(exampleQuery)