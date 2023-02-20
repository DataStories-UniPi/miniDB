
import copy


def rule1(query):
    '''
    Given a query, apply the transformation rule
        σ q1∧q2 (R) = σ q1(σ q2 (R))
    query: the query to transform
    '''
    if 'select' in query.keys() and isinstance(query['from'],dict) and 'select ' in query['from'].keys():
        equiv_query = copy.deepcopy(query)
        # Split the condition into two parts
        condition1, condition2 = equiv_query['where'].split('and')
        equiv_query['where'] = condition1
        equiv_query['from'] = {'select': '*', 'from': equiv_query['from'], 'where': condition2,'distinct': None, 'orderby': None,'limit': None,'desc': None}
        return equiv_query
    return None
    
def rule2(query): 
    '''
    Rule 2: σθ1(σθ2(R)) = σθ2(σθ1(R))
    '''
    if 'select' in query.keys() and 'select' in query['from'].keys():
        equiv_query = copy.deepcopy(query)
        condition1=equiv_query['where']  # θ1
        condition2=equiv_query['from']['where'] # θ2
        equiv_query['where'] = condition2
        equiv_query['from']['where'] = condition1 
        return equiv_query
    return None

def rule3(query):
    '''
    Rule 3b: σθ1(R ⋈θ2 S) = R ⋈(θ1^θ2) S 
    '''
    if 'select' in query.keys() and (isinstance(query['from'], dict) and 'join' in query['from'].keys()) and query['where'] is not None and query['from']['on'] is not None:
        equiv_query = copy.deepcopy(query) 
        condition1=equiv_query['where']  # θ1
        condition2=equiv_query['from']['on'] # θ2
        equiv_query['where'] = None
        equiv_query['from']['on'] = condition1 + ' and ' + condition2 # θ1^θ2 as the join condition
        return equiv_query
    return None

def rule4(query):
    '''
    Given a query, apply the transformation rule
        R ⋈q S = S ⋈q R
    '''
    
    if 'join' in query.keys():
        equiv_query = copy.deepcopy(query)
        left=equiv_query['from']['left']
        right=equiv_query['from']['right']
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
    pass


def rule6(query):
    '''
    Given a query, apply the transformation rules
        case 1: σ q1(R ⋈q S) = σ q1(R) ⋈q S 
        case 2: σ q1(R ⋈q S) = R ⋈q σ q1(S)
        case 3: σ q1^q2 (R ⋈q S) = σ q1(R) ⋈q σ q2 (S)
    '''
    equivalent_queries = []
    if 'select' in query.keys() and ((isinstance(query['from'], dict) and 'join' in query['from'].keys())):
        if query['where'] is not None and 'and' in query['where']:
            equiv_query= copy.deepcopy(query)
            # Split the condition into two parts
            condition1, condition2 = equiv_query['where'].split('and')
            # Apply the rule(case of double select condition: condition1 corresponds to left table and condition2 corresponds to right table)
            subquery1 = {'select': '*', 'from': query['from']['left'], 'where': condition1, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            subquery2 = {'select': '*', 'from': query['from']['right'], 'where': condition2, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            equiv_query['from']['left']=subquery1
            equiv_query['from']['right']=subquery2
            equiv_query['where']=None
            equivalent_queries.append(equiv_query)
        
        # Apply the rule(case: condition corresponds only to left table)
        equiv_query= copy.deepcopy(query)
        subquery1 = {'select': '*', 'from': query['from']['left'], 'where': query['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
        equiv_query['from']['left']=subquery1
        equiv_query['where']=None
        equivalent_queries.append(equiv_query)
        # Apply the rule(case: condition corresponds only to right table)
        equiv_query= copy.deepcopy(query)
        subquery2 = {'select': '*', 'from': query['from']['right'], 'where': query['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
        equiv_query['from']['right']=subquery2
        equiv_query['where']=None
        equivalent_queries.append(equiv_query)
        return equivalent_queries
    return None
    
def equiv_recursive(query, equiv_queries=None, lastrule=None, visited=None):
    # keep track of the visited queries as a frozen set
    if equiv_queries is None:
        equiv_queries = []
    if query in equiv_queries:
        return equiv_queries
    equiv_queries.append(query)
    for rule in [rule1, rule2, rule3, rule4, rule5, rule6]:
        equiv_query = rule(query)
        if equiv_query is not None and lastrule != rule:
            lastrule = rule
            if rule == rule6:
                for q in equiv_query:
                    equiv_queries.append(q)
                    equiv_recursive(q, equiv_queries, lastrule, visited)
            else:
                equiv_queries.append(equiv_query)
                equiv_recursive(equiv_query, equiv_queries, lastrule, visited)
    return equiv_queries

def equiv_print(query):
    equiv_queries=equiv_recursive(query)
    for count,q in enumerate(equiv_queries):
        print ("\n"+"Query "+str(count+1)+": \n"+str(q)+"\n")

# Example dictionary
exampleQuery = {'select': '*', 'from': {'select': '*', 'from': 'R', 'where': 'a > 5', 'distinct': None, 'orderby': None,'limit': None,'desc': None}, 'where': 'b < 10', 'distinct': None, 'orderby': None,'limit': None,'desc': None}
# Second example dictionary with join
exampleQuery2 = {'select': '*', 'from': {'join': 'inner', 'left': 'instructor', 'right': 'department', 'on': 'instructor.dept_name=department.dept_name'}, 'where': 'not instructor.dept_name=biology ', 'distinct': None, 'order by': None, 'limit': None, 'desc': None}
# Third example dictionary with join and 'and' condition
exampleQuery3 = {'select': '*', 'from': {'join': 'inner', 'left': 'instructor', 'right': 'department', 'on': 'instructor.dept_name=department.dept_name'}, 'where': 'not instructor.dept_name=biology and instructor.salary>60000', 'distinct': None, 'order by': None, 'limit': None, 'desc': None}
# Print query
#equiv_print(exampleQuery3)