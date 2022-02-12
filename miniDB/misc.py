import operator
import datetime
import re

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
           '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '=': operator.eq,
            'in':in_operator,
            'between':between_operator,
            'like':like_operator}
    
    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    condition = condition.replace(' ','') # remove all whitespaces
    ops = {'>': operator.gt,
           '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '=': operator.eq,
            'in':in_operator,   # The in operator allows to specify multiple values in a WHERE clause.
            'between':between_operator, # The between operator selects values within a given range. The values can be numbers, text, or dates.
            'like':like_operator}   # The LIKE operator is used in a WHERE clause to search for a specified pattern in a column.
                                    # There are two wildcards often used in conjunction with the LIKE operator:
                                    # ~ The percent sign (%) represents zero, one, or multiple characters.
                                    # ~ The underscore sign (_) represents one, single character.

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            return splt[0], op_key, splt[1]

# in_operator function returns the rows that satisfy the given WHERE clause using IN(value1,value2,value3,...).
def in_operator(x,y):
    return x in y

# between_operator function returns the rows that satisfy the given WHERE clause using BETWEEN value1 and value2.
def between_operator(x,y):
    return x <= y 

# like_operator function returns the rows that satisfy the given WHERE clause using LIKE followed by a regular expression.
def like_operator(x,y):
    # Convert SQL LIKE wildcards to regular expression.
    regex =re.compile(y.replace('%','@').replace('_',"&"))
    if(re.fullmatch(regex,x)):
        return True
