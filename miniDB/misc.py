import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    '''
    Added not equal operator
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                '!=': operator.ne
           }

    try:
        return ops[op](a,b) # does the operation and returns a boolean result
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    '''
       Added not equal operator
    '''
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           '!=': operator.ne
           }

    for op_key in ops.keys():
        if('between' in condition):
            splt = condition.split('between')
            left, right = splt[0].strip(), splt[1].strip()
            return left, 'between', right
        if('and' in condition):
            splt = condition.split('and')
            print('and found ',splt[0],splt[1] )
            return splt
        if('or_condition' in condition):
            splt = condition.split('or_condition')
            print('or found ', splt[0], splt[1])
            return splt
        else:
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
    '''
        Added not equal operator as the reverse counterpart of the equal operator
    '''
    return {
        '>' : '<',
        '>=' : '<=',
        '<' : '>',
        '<=' : '>=',
        '=' : '!='
    }.get(op)
