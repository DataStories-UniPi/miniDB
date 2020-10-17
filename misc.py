import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '==': operator.eq}

    return ops[op](a,b)

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '==': operator.eq,
           '>': operator.gt,
           '<': operator.lt}

    for op_key in ops.keys():
        if len(splt:=condition.split(op_key))>1:
            return splt[0], op_key, splt[1]
