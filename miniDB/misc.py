#CHANGES BY P16058 P16197


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
                #addition to implement NOT operator
                'not'.casefold():operator.ne,
                #addition to implement BETWEEN operator
                'between'.casefold():between_op}

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
           #addition to implement NOT operator
           'not'.casefold():operator.ne,
           #addition to implement BETWEEN operator
           'between'.casefold():between_op}

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


#BETWEEN operator function
def between_op(column_value,between_range):
    between=between_range.split(',')
    res=False
    if(type(column_value)==int or type(column_value)==float):
        #numerical comparison
        res=(float(between[0])<column_value and float(between[1])>column_value)
    else:
        #string comparison
        res=between[0]<column_value and between[1]>column_value
    return res

