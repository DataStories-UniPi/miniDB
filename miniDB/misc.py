import operator


def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq, 
                'between':between # Between operator
                }

    try:
         return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between':between # Between operator
           }
        
    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()

            if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')
            elif ' ' in right: # If it has whitespaces but no leading and trailing double quotes, throw.
                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

            return left, op_key, right
    

def reverse_op(op):
    '''
    Reverse the operator given
    '''
    return {
        '>' : '<',
        '>=' : '<=',
        '<' : '>',
        '<=' : '>=',
        '=' : '='
    }.get(op)

def between(a,b):
    '''
       Returns a boolean that shows if a is in the specified range [b[0], b[1]].
       It works for numerical and strings.
    '''
    range_list = b.split('_and_') # Split where condition value to get the range
    try:   # For numerical data convert string to float    
        min = float(range_list[0])
        max = float(range_list[1])
        return min <= a <= max # <= because between is inclusive in theory
    except: # If conversion to float does not succeed because range_list contains strings
        min = range_list[0]
        max = range_list[1]
        return min <= a <= max # <= because between is inclusive in theory