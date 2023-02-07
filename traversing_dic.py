def evaluate_condition(clause):
    if isinstance(clause, str):
        # clause is a string, evaluate as a boolean expression
        return clause
    elif 'and' in clause:
        left = evaluate_condition(clause['and']['left'])
        right = evaluate_condition(clause['and']['right'])
        # evaluate left and right and return the result
        return left + ' and ' + right
    elif 'or' in clause:
        left = evaluate_condition(clause['or']['left'])
        right = evaluate_condition(clause['or']['right'])
        # evaluate left and right and return the result
        return left + ' or ' + right
    elif 'not' in clause:
        # evaluate the clause and return the result
        return 'not ' + evaluate_condition(clause['not'])
    elif 'between' in clause:
        # evaluate the clause and return the result
        return clause['column'] + ' between ' + evaluate_condition(clause['between'])


where_clause = {'or': {'left': {'and': {'left': 'id=5', 'right': 'age=25'}},
                         'right': "course='maths'"}}

where_clause = {'or': {'left': {'and': {'left': 'name="sakis"',
                                   'right': {'column': 'id',
                                             'between': {'and': {'left': '5',
                                                                 'right': '20'}}}}},
                  'right': 'age=5'}}

where_clause = {'or': {'left': {'not': 'a=1'},
                  'right': {'or': {'left': {'and': {'left': {'not': {'and': {'left': {'not': 'b=5'},
                                                                             'right': {'not': {'or': {'left': {'not': 'd=1'},

         'right': {'not': 'e=5'}}}}}}},
                                                    'right': {'not': 'c=2'}}},
                                   'right': {'and': {'left': {'not': 'q=1'},
                                                     'right': {'not': {'or': {'left': {'not': 'h=4'},
                                                                              'right': {'not': 't=8'}}}}}}}}}}
result = evaluate_condition(where_clause)
print(result)
