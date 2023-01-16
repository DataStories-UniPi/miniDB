import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {     '>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
<<<<<<< HEAD
                '!=': operator.ne, # not equal operator !=
                '=': operator.eq,} 
=======
                '=': operator.eq,
                '<>': operator.ne}
>>>>>>> bcace31 (or and not op)

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '!=': operator.ne,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,} 

    for op_key in ops.keys():
        splt=condition.split(op_key)
<<<<<<< HEAD
        print(splt)
        if len(splt)>1: # operator has been found
            left, right = splt[0].strip(), splt[1].strip() 
=======
        #print("split is: ")
        #print(splt)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()
>>>>>>> bcace31 (or and not op)

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


def not_op(op):
   '''
   Handle operator not, by changing the operator given
   '''
   return {
        '>' : '<=',
        '>=' : '<',
        '<' : '>=',
        '<=' : '>',
        '=' : '<>'
    }.get(op)

def split_not_condition(condition): # not salary > 50000 
    #condition = condition[4:]       # salary > 50000
    splt = condition.split(' ')
    #print(splt)
    op_key = not_op(splt[2])  
    left, right = splt[1].strip(), splt[3].strip()

    if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
        right = right.strip('"')
    elif ' ' in right: # If it has whitespaces but no leading and trailing double quotes, throw.
        raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')
    if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
        raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

    return left, op_key, right


