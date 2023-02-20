import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq}

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}

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
        
def split_expression(expression):
    exprs = (('not', operator.not_),
            ('and', operator.and_),
             ('or', operator.or_))
    
    for exp_key, exp_op in exprs:
        splt=expression.split(exp_key)
        if(len(splt) == 1):
            condition = splt[0].strip("()")
            return split_condition(condition)
        else:
            for i in splt:
                split_expression(i)

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
    Reverse the operator given with not logic
    '''
    return {
        '>' : '<=',
        '>=' : '<',
        '<' : '>=',
        '<=' : '>',
        '=' : '!='
    }.get(op)
