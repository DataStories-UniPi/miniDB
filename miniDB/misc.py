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
            if ' ' in right:
                if right[0] == '"' == right[-1]:
                    return left, op_key, right.strip('"')
                else:
                    raise Exception(f'Invalid condition ({condition}). Value must be enclosed in double quotation marks if it contains whitespaces.')
            else:
                if right[0] == '"' == right[-1]:
                    return left, op_key, right.strip('"')
                else:
                    return left, op_key, right
