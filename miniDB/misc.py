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
        a = type(b)(a)  # Confirm that they are of the same type
        return ops[op](a, b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False


def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}

    for op_key in ops.keys():
        splt = condition.split(op_key)
        if len(splt) > 1:
            left, right = splt[0].strip(), splt[1].strip()

            if right[0] == '"' == right[-1]:  # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')
            elif ' ' in right:  # If it has whitespaces but no leading and trailing double quotes, throw.
                raise ValueError(
                    f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find(
                    '"') != -1:  # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(
                    f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

            return left, op_key, right


"""
Splits condition into the condition column and the values around the and keyword
"""
def split_condition_between(condition):
    splt = condition.split(' between ') # Split at between and get then get the right and left values around and and then get the condition column that is the left side of between
    if len(splt) > 1:
        values = splt[1].split(' and ')[0:2]
        condition_column = splt[0]

        if values[0] == '"' == values[-1]:  # If the value has leading and trailing quotes, remove them.
            values = values.strip('"')
        elif ' ' in values:  # If it has whitespaces but no leading and trailing double quotes, throw.
            raise ValueError(
                f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

        if values[0].find('"') != -1 or values[1].find(
                '"') != -1:  # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
            raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

        return condition_column, values

"""
Compares the three values and if a between b and c returns true else it returns false
"""
def get_op_between(a, b, c):  # Funtion the takes checks if a is between b and c
    try:
        a = type(b)(a)  # Confirm that they are of the same type
        if b <= c: # Check if b and c are in the right order
            if b <= a <= c:
                return True
            else:
                return False
        else:
            if c <= a <= b:
                return True
            else:
                return False
    except TypeError:
        return False


def reverse_op(op):
    '''
    Reverse the operator given
    '''
    return {
        '>': '<',
        '>=': '<=',
        '<': '>',
        '<=': '>=',
        '=': '='
    }.get(op)
