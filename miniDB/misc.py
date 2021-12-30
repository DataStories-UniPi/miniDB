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
        return ops[op](a,b) #mporoume na poume px operator.lt(2,3) kai ayto epistrefei true
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    condition = condition.replace(' ','') # remove all whitespaces
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}

    for op_key in ops.keys(): #psaxnoume poio operator yparxei sto condition kai epistrefoume ta 3 kommatia toy condition otan to vroume(len(splt)>1 mono otan vroume to operator)
        splt=condition.split(op_key)
        if len(splt)>1:
            return splt[0], op_key, splt[1]
