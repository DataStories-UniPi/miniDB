import operator

def operator_between(value, condition):
    begin, end = condition.split('and')

    begin = begin.strip()
    end = end.strip()

    begin = begin.replace("'","")
    end = end.replace("'","")

    if (begin.isnumeric() and end.isnumeric()):
        begin = int(begin)
        end = int(end)
        if (value >= begin) and (value <= end):
            return True

    elif (not(begin.isnumeric() or end.isnumeric())): 
        if (value >= begin) and (value <= end):
            return True

    else:
        raise Exception("Values must be of the same type!")

    return False


def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                'between': operator_between
                }

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
           'between': operator_between
           }

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()
        
            if op_key == 'between':
                begin,end = right.split('and')
                begin = begin.strip()
                end = end.strip()

                if (begin[0] == '"' == begin[-1]) or (end[0] == '"' == end[-1]):
                    begin = begin.strip('"',"")
                    end = end.strip('"',"")
                elif ( ' ' in begin) or ( ' ' in end):
                    raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')
                if (begin.find('"') != -1) or (end.find('"') != -1): # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                    raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')
                return left, op_key, right
                
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
