import mdb
import copy 

class Node:
    '''
    To create an Estimated Subtree Cost -> Its a cumulative cost associated with the whole subtree up to the node.
    *Not implemented
    '''
    def __init__(self):
        self.next_node = None
        self.parent_node = None
        self.operation = ''
        self.args = ''

    def estimate_cost_of_tree(self) -> int:
        pass



class Optimizer:
    '''
    Optimizer object that contains equivalent queries and plans, based on the equivalence rules taught in
    DBMS Course in 06-QueryProcOptim.pdf, so it represents a general approach to query optimization.
    '''

    def __init__(self,eq_queries=None, eq_plans=None, rule=None):
        self.eq_queries = eq_queries
        self.eq_plans = eq_plans
        self.rule = rule

    def _get_eq_query_plan(self,eq_query):
        eq_plans = []
        eq_plans.append(eq_query)
        return eq_plans


    def _build_equivalent_query_plan(self,ogplan):           
        '''
        Create query plans based on the equivalence rules in dbms.
        '''
        
        eq_queries = [] # list with the equivalent queries to be created
        notchecked_queries = [ogplan]

        for q in notchecked_queries:  # for each query that is not still checked
            temp_query = [q]
            
            for key in q.keys():
                if isinstance(q[key], dict):
                    eq_sub_queries = self._build_equivalent_query_plan(q[key]) # recursive call

                    for subq in eq_sub_queries:
                        temp = q
                        q[key] = subq
                        if temp not in eq_queries and temp not in temp_query:
                            temp_query.append(temp)
                            

            # send the query through all the equivelant rules for check
            # gives back one query plan**
            rule = self.rule1(q)
            rule.append(self.rule2(q))
            rule.append(self.rule3a(q))
            rule.append(self.rule4(q))
            rule.append(self.rule6(q))
            #eq_queries.append(q)

            for temp in rule:
                if temp not in eq_queries and temp not in temp_query:
                    temp_query.append(temp) # add temporary subquery to be checked

            eq_queries.append(q)
            notchecked_queries.remove(q) # remove the query that went through the rules

            for temp in temp_query:
                if temp not in eq_queries and temp not in temp_query:
                    temp_query.append(temp)

        #self._get_eq_query_plan(eq_queries) 
        return eq_queries

    def rule1(self,query_plan):
        '''
        Rule 1: σθ1∧θ2 (R) = σθ1(σθ2(R))
        '''
        eq_queries = []
        temp_queries = {} # creates a temporary dictionary 

        
        if 'select ' in query_plan.keys() and isinstance(query_plan['from'],dict) and 'select ' in query_plan['from'].keys(): # for nested selection
            if not (query_plan['distinct'] == False and query_plan['where'] != query_plan['from']['where']):
                
                temp_queries = dict(query_plan)

                # change query syntax 
                if (temp_queries['from'])['where'] != None:
                    temp_queries['where'] += ' and ' + temp_queries['from']['where'] # for each item in the select keyword
                if (temp_queries['order by'] == None):
                    temp_queries['order by'] = query_plan['from']['order by']
                if (temp_queries['desc'] == None):
                    temp_queries['desc'] = (query_plan['from'])['desc']
                if temp_queries['limit']!=None or (query_plan['from'])['limit']!=None:
                    temp_queries['limit'] = min(i for i in [temp_queries['limit'] , query_plan['from']['limit']] if i is not None  )

                temp_queries['from'] = temp_queries['from']['from']

                eq_queries.append(temp_queries) # add equivalent query to list

        return eq_queries

    def rule2(self,query_plan):
        '''
        Rule 2: σθ1(σθ2(R)) = σθ2(σθ1(R))
        '''

        #eq_queries = []
        temp_queries = {}

        if 'select' in query_plan.keys() and isinstance(query_plan['from'],dict) and 'select' in query_plan['from'].keys(): # in case of double select
            if not (query_plan['distinct'] == False and query_plan['where'] != query_plan['from']['where']):
                if query_plan['limit'] == None and query_plan['from']['limit'] == None:
                    temp_queries = dict(query_plan)
                    temp_queries['where'] = temp_queries['from']['where']
                    temp_queries['from']['where'] = query_plan['where']
                    return [temp_queries]

        return []

    def rule3a(self,query_plan):
        '''
        Rule 3a: σθ1 (R ⋈θ2 S) = R ⋈θ1∧θ2 S
        '''

        temp_queries = {}

        if 'select' in query_plan.keys() and isinstance(query_plan['from'],dict) and 'join' in query_plan['from'].keys(): # join with select 
            if query_plan['select']=='*' and query_plan['distinct']==None and query_plan['order by']==None and query_plan['limit']==None and query_plan['desc']==None:
                temp_queries = dict(query_plan['from'])
                if query_plan['where'] is not None:
                    temp_queries['on'] += ' and ' + query_plan['where']
                return temp_queries

        return []

    def rule4(self,query_plan):
        '''
        R ⋈θ S = S ⋈θ R
        '''
        
        temp_queries = {}
        if 'join' in query_plan.keys():
            
            temp_queries = copy.deepcopy(query_plan)
            temp_queries['left'] = temp_queries['right']
            temp_queries['right'] = query_plan['left']
            print("Equivalent query:", temp_queries)
            return temp_queries

        return []
    
    def rule6(self, query_plan):
        '''
        σθ1(R ⋈θ S) = σθ1(R) ⋈θ S
        '''

        equivalent_queries = []
        if 'select' in query_plan.keys() and isinstance(query_plan['from'],dict) and 'join' in query_plan['from'].keys():
            if query_plan['where'].find('and') != -1:
                # Split the condition into two parts
                condition1, condition2 = query_plan['where'].split('and')
                # Apply the rule(case of double select condition: condition1 corresponds to left table and condition2 corresponds to right table)
                subquery1 = {'select': '*', 'from': query_plan['from']['left'], 'where': condition1, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
                subquery2 = {'select': '*', 'from': query_plan['from']['right'], 'where': condition2, 'distinct': None, 'orderby': None,'limit': None,'desc': None}
                equiv_query= copy.deepcopy(query_plan)
                equiv_query['from']['left']=subquery1
                equivalent_queries['from']['right']=subquery2
                equivalent_queries.append(equiv_query)
            
            # Apply the rule(case: condition corresponds only to left table)
            subquery1 = {'select': '*', 'from': query_plan['from']['left'], 'where': query_plan['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            equiv_query= copy.deepcopy(query_plan)
            equiv_query['from']['left']=subquery1
            equivalent_queries.append(equiv_query)
            # Apply the rule(case: condition corresponds only to right table)
            subquery2 = {'select': '*', 'from': query_plan['from']['right'], 'where': query_plan['where'], 'distinct': None, 'orderby': None,'limit': None,'desc': None}
            equiv_query= copy.deepcopy(query_plan)
            equiv_query['from']['right']=subquery2
            equivalent_queries.append(equiv_query)
        return equivalent_queries


op = Optimizer()
for item in op._build_equivalent_query_plan(mdb.interpret('select * from instructor inner join department on instructor.dept_name=department.dept_name where dept_name!=biology')):
    print(item)
