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
           '<': operator.lt,
           '!=': operator.ne}

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


# Conversion of "between" operator into "greater than or equal AND less than or equal"
def convert_between_condition(condition):
    betweens = condition.split('between')
    new_condition=""
    left=betweens[0].split()
    for i in range(0, len (left) - 1):
        new_condition +=left[i] + " "

    for i in range(len(betweens) -1):
        left = betweens[i].split()
        right = betweens[i + 1].split()
        new_condition+= left[-1] +" >= " + right[0] + " and " + left[-1] + " <= " + right[2] + " "

    for i in range(3, len(right)):
        new_condition += right[i] + " "

    return new_condition.strip()

def reverse_not (condition):
    left, op_key, right = split_condition(condition)
    op_key = {
        '>': '<=',
        '>=': '<',
        '<': '>=',
        '<=': '>',
        '=': '!=',
        '!=': '='
    }.get(op_key)
    condition = left + " " + op_key + " " + right
    return condition
