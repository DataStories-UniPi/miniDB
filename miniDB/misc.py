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
    condition = condition.replace(' ','') # remove all whitespaces
    condition = condition.split('and')
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}
    left = []
    op = []
    right = []
    for statement in condition:
        for op_key in ops.keys():
            splt = statement.split(op_key)
            if len(splt) > 1:
                left.append(splt[0])
                op.append(op_key)
                right.append(splt[1])

    return left, op, right
