import operator


def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '!=': operator.ne,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between': between,
           'BETWEEN': between
           }

    try:
        # print(ops[op](a, b))
        return ops[op](a, b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typeError
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '!=': operator.ne,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between': between,
           'BETWEEN': between
            }

    for op_key in ops.keys():
        if (op_key == '>=' or op_key == '<=' or op_key == '!=' or op_key == '=' or op_key == '>' or op_key == '<') and (op_key in condition):
            splt = condition.split(op_key)
            if len(splt) > 1:
                left, right = splt[0].strip(), splt[1].strip()
                if right[0] == '"' == right[-1]:  # If the value has leading and trailing quotes, remove them.
                    right = right.strip('"')
                elif ' ' in right:  # If it has whitespaces but no leading and trailing double quotes, throw.
                    raise ValueError(
                        f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')
                if right.find('"') != -1:  # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                    raise ValueError(
                        f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')
                return left, op_key, right
        elif (op_key == 'between' or op_key == 'BETWEEN') and (op_key in condition):
            split_cond1 = condition.split('between')
            try:
                if " and " not in split_cond1[1]:
                    raise ValueError('The query BETWEEN needs the operator and to separate the arguments. Try again!')
                values = tuple((split_cond1[1].strip()).split(' and '))
                column_cond = split_cond1[0].strip()
                left = column_cond
                right = values
                op_key = 'between'
                return left, op_key, right
            except:
                raise ValueError('A where query with the BETWEEN operator has the following format:'
                                 '... where column_name between/BETWEEN value1 and value2. Try again!')


def between(a, b):
    a = str(a)
    value1, value2 = b
    # between statement for strings
    if type(value1) == str and type(value2) == str and not value1.isdigit() and not value2.isdigit() and not a.isdigit():
        return a if value1 <= a <= value2 else None
    # between statement for numbers
    elif value1.isdigit() and value2.isdigit() and a.isdigit():
        value1 = float(value1)
        value2 = float(value2)
        a = float(a)
        return a if value1 <= a <= value2 else None
    else:
        raise ValueError('You are trying to compare numbers and characters. Try again!')


# updated reverse_op method, to include all operators and their opposites
def reverse_op(op):
    '''
    Reverse the operator given
    '''
    return {
        '>=': '<',
        '<=': '>',
        '!=': '=',
        '=': '!=',
        '>': '<=',
        '<': '>='
    }.get(op)





