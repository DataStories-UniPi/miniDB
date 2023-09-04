def ra_eq1(query):
    '''
        σθ1∧θ2(E) = σθ1(σθ2(E))
    '''
    #Έλεγχος για το αν υπάρχει condition στο where και αν υπαρχει και and
    if query['where'] is not None and query['where'].find('and') != -1:
        #Split το condition σε 2 μέρη και ανακατασκευή.
        condition1, condition2 = query['where'].split('and')
        equiv_query = query.copy()
        equiv_query['where'] = condition1
        equiv_query['from'] = {'select': '*', 'from': equiv_query['from'], 'where': condition2,'distinct': None, 'orderby': None,'limit': None,'desc': None}
        return equiv_query
    return None

def ra_eq2(query):
    '''
        E1 inner join θ E2 ---> E2 inner join θ 
    '''
    if 'from' in query and 'join' in query['from']:
        left = query['from']['left']
        right = query['from']['right']
        equiv_query = query.copy()
        equiv_query['from']['left'] = right
        equiv_query['from']['right'] = left
        return equiv_query
    else:
        return query

def ra_eq3(query):
    '''
        select θ (Ε1 Inner Join θ1 Ε2) ----> (select θ Ε1) Inner Join θ1 Ε2
    '''
    if 'from' in query and 'join' in query['from'] and query['from']['join'] == 'inner':
        left_table = query['from']['left']
        right_table = query['from']['right']
        on_condition = query['from']['on']
        where_condition = query['where']

        #Ελεγχος για το table στο οποίο ανήκει το condition (π.χ classroom.capacity το condition ανήκει στο table classroom)
        if where_condition and where_condition.startswith(f"{left_table}."):
            nested_left = {
                'select': query['select'],
                'from': left_table,
                'where': where_condition
            }
            nested_right = right_table
        elif where_condition and where_condition.startswith(f"{right_table}."):
            nested_left = left_table
            nested_right = {
                'select': query['select'],
                'from': right_table,
                'where': where_condition
            }
        else:
            nested_left = left_table
            nested_right = right_table

        transformed_query = {
            'select': query['select'],
            'from': {
                'join': 'inner',
                'left': nested_left,
                'right': nested_right,
                'on': on_condition
            },
            'where': None,  # Αφου το where condition έχει ήδη γίνει apply πιο πάνω.
            'distinct': query['distinct'],
            'order by': query['order by'],
            'limit': query['limit']
        }
        print("OEOOOO")
        print(transformed_query)
        return transformed_query
    else:
        return query

def ra_eq4(query):
    '''
    Given a query, apply the transformation rule
    select θ1 AND θ2 (Ε1 inner join Ε2) -> (select θ1 Ε1) INNER JOIN (select θ2 Ε2)
    '''
    if 'from' in query and 'join' in query['from'] and query['from']['join'] == 'inner':
        left_table = query['from']['left']
        right_table = query['from']['right']
        on_condition = query['from']['on']
        where_condition = query['where']

        theta1, theta2 = where_condition.split('and')
        
        transformed_query_left = {
            'select': query['select'],
            'from': left_table,
            'where': "",
            'distinct': query['distinct'],
            'order by': query['order by'],
            'limit': query['limit']
        }

        transformed_query_right = {
            'select': query['select'],
            'from': right_table,
            'where': "",
            'distinct': query['distinct'],
            'order by': query['order by'],
            'limit': query['limit']
        }
        
        #Λοοπ μέσα από όλα τα conditions και κάνει assign το κάθε where condition στο αντίστοιχο table
        for condition in (theta1, theta2):
            condition = condition.strip()
            if left_table in condition:
                transformed_query_left['where'] += f"{condition}"
            elif right_table in condition:
                transformed_query_right['where'] += f"{condition}"
                
        transformed_query_left['where'] = transformed_query_left['where'].rstrip('and')
        transformed_query_right['where'] = transformed_query_right['where'].rstrip('and')


        final_transformed_query = {
            'select': '*',
            'from': {
                'join': 'inner',
                'left': transformed_query_left,
                'right': transformed_query_right,
                'on': on_condition
            },
            'where': None,
            'distinct': query['distinct'],
            'order by': query['order by'],
            'limit': query['limit']
        }

        return final_transformed_query

    return query