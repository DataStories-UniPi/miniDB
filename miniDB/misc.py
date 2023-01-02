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
           '!=': operator.ne
           }

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '!=': operator.ne,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt
           }

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


def check_logops(condition):
    log_ops = {'not ': operator.not_,
               ' and': operator.and_,
               ' or': operator.or_,
               }

    for log_op in log_ops.keys():
        logsplt = condition.split(log_op)
        if log_op == 'not ' and len(logsplt) > 1:
            logsplt.pop(0)
            logsplt = logsplt[0]
            return logsplt, log_op
        elif len(logsplt) > 1:
            return logsplt, log_op

    return condition, 'none'



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

def oppose_op(op):
    '''
    Oppose the operator given
    '''
    return {
        '>': '<=',
        '>=': '<',
        '<': '>=',
        '<=': '>',
        '=': '!=',
        ' and': ' or',
        ' or': ' and',
        'none': 'none'
    }.get(op)
