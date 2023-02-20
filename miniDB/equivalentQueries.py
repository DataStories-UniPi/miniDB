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
        equiv_query = query.copy()
        equiv_query['where'] = condition1
        equiv_query['from'] = {'select': '*', 'from': equiv_query['from'], 'where': condition2,'distinct': None, 'orderby': None,'limit': None,'desc': None}
        return equiv_query
    return None
def rule2(query):

def rule4(query):
    '''
    Given a query, apply the transformation rule
        R ⋈q S = S ⋈q R
    '''
    
    if 'select' in query.keys() and query['from'].find('join') != -1:
        left=query['from']['left']
        right=query['from']['right']
        equiv_query = query.copy()
        equiv_query['from']['left'] = right
        equiv_query['from']['right'] = left
        return equiv_query
    
def rule5(query):
    '''
    Given a query, apply the transformation rules:
       -(only for natural join)
        (R ⋈ S) ⋈ T = R ⋈ (S ⋈ T)
    
       -( θ-join )
        (R ⋈q1 S) ⋈ q2^q3 T = R ⋈ q1^q3 (S ⋈ q2 T)
    '''
    # multiple joins are not supported by minidb for now
    if 'select' in query.keys() and query['from'].find('join') != -1 and query['where'] is not None: 
        condition=query['where']
        if condition.find('and') != -1:
            condition1=query['join']['on']
            condition2, condition3 = condition.split('and')
            # Apply the rule
            equiv_query = query.copy()
        return query
    


def rule6(query):
    '''
    Given a query, apply the transformation rules
        case 1: σ q1(R ⋈q S) = σ q1(R) ⋈q S 
        case 2: σ q1(R ⋈q S) = R ⋈q σ q1(S)
        case 3: σ q1^q2 (R ⋈q S) = σ q1(R) ⋈q σ q2 (S)
    '''
    equivalent_queries = []
    if 'select' in query.keys() and query['from'].find('join') != -1:
        if query['where'].find('and') != -1:
            # Split the condition into two parts
            condition1, condition2 = query['where'].split('and')
            # Apply the rule(case of double select condition: condition1 corresponds to left table and condition2 corresponds to right table)
            subquery1 = {'select': '*', 'from': query['from']['left'], 'where': condition1, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            subquery2 = {'select': '*', 'from': query['from']['right'], 'where': condition2, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            equiv_query= query.copy()
            equiv_query['from']['left']=subquery1
            equivalent_queries['from']['right']=subquery2
            equivalent_queries.append(equiv_query)
        
        # Apply the rule(case: condition corresponds only to left table)
        subquery1 = {'select': '*', 'from': query['from']['left'], 'where': query['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
        equiv_query= query.copy()
        equiv_query['from']['left']=subquery1
        equivalent_queries.append(equiv_query)
        # Apply the rule(case: condition corresponds only to right table)
        subquery2 = {'select': '*', 'from': query['from']['right'], 'where': query['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
        equiv_query= query.copy()
        equiv_query['from']['right']=subquery2
        equivalent_queries.append(equiv_query)
    return equivalent_queries
    

      