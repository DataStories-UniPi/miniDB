import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
     #EVALA STO ops TO AND ,NOT ,OR
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                '!=':operator.ne,
                '&&':operator.and_,
                '||':operator.or_
                }

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False


    #EVALA STO SPILT TO AND ,NOT ,OR  
def split_condition(condition):
    ops = {'&& ':operator.and_,
           'and ':operator.and_,
           'or ':operator.or_,
           '|| ':operator.or_,
           '>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt
           }

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()

            print(len(right))
            print("/n")
            i=0

            for n in left:
                if n=='"':
                    left=left.replace(n,'')
            i=i+1

            i=0
            for n in right:
                if n=='"':
                    right=right.replace(n,'')
            i=i+1

            #if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
            #   right = right.strip('"')
            if ' ' in right: # If it has whitespaces but no leading and trailing double quotes, throw.
                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')
            print(left)
            print("/n")

            print(right)
            print("/n")
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
        '=' : '=',
    }.get(op)
