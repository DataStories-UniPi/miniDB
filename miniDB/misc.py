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
            '!=': operator.ne,
            'between': lambda a,b: (a>=b[0] and a<=b[1]) or (a>=b[1] and a<=b[0]),  # regardless if the first or second number is larger, the between clause will run as intended
            'not between': lambda a,b: (a>b[0] and a>b[1]) or (a<b[0] and a<b[1])
           }

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'not': reverse_op,
           '>=': operator.ge,
           '<=': operator.le,
           '!=': operator.ne,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between': lambda a,b: (a>=b[0] and a<=b[1]) or (a>=b[1] and a<=b[0]),
           }

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()

            if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')

            if op_key=='not' and 'between' not in right:    # If theres 'not' without 'between', we sent the operator to the reverse function
                if right[0:2] in ops.keys():
                    return left, ops[op_key](right[0:2]),right[2:]
                elif right[0] in ops.keys():
                    return left, ops[op_key](right[0]), right[1:]
                else:                                       # if no operator is given assume that is an '=' (e.g. 'id not 10')
                    return left, ops[op_key]('='), right
            elif op_key=='not' and 'between' in right:                             #If theres 'not' with 'between', we sent the operator to the reverse function and we do the same prep for the 'right' variable as with the between operator
                right = right.removeprefix('between')
                return left, ops[op_key]('between'), right.split(',')
            elif op_key=='between':         # instead of returning a single number, a list of 2 values (upper and lower limit) must be returned in this case
                return left, op_key, right.split(',')

            elif ' ' in right and ' or ' not in right: # If it has whitespaces but no leading and trailing double quotes, throw.
                if ' and ' not in right:
                    raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

            return left, op_key, right

def reverse_op(op):
    '''
    Reverse the operator given
    '''
    return {
        '>' : '<=',
        '>=' : '<',
        '<' : '>=',
        '<=' : '>',
        '=' : '!=',
        '!=': '=',
        'between': 'not between'
    }.get(op)
