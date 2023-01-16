import operator

def not_op(op):
    '''
    Get the negated operator of the received op
    '''
    return {
        '>' : '<=',
        '<' : '>=',
        '>=' : '<',
        '<=' : '>',
        '=': '<>'
        
    }.get(op)

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                '<>': operator.ne}

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition,notcheck=False):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           '<>': operator.ne}

    found = False # used to break the loop when we find the operator
    if notcheck:
        if ' ' in condition: # the condition must have whitespaces so we can split it there
            list_condition = condition.split() 
            #correct syntax: salary >= 67000
        else:
            raise SyntaxError(f'Invalid format: {condition}\nCorrect syntax: Condition must contain whitespaces, e.g. column_name >= value.')

        print(list_condition)
        for op_key in ops.keys():
            for i,n in enumerate(list_condition):
                if n == op_key:
                    print(op_key)
                    not_op_key = not_op(op_key) # negate the operator
                    list_condition[i] = str(not_op_key) # replace operator in the list 
                    found = True
                    new_op = not_op_key # used later to split the condition in the correct and reversed operator
                    print(not_op(op_key))              
            if found: # if the operator is found and changed
                # this is used because the function would otherwise continue to reverse the already reversed operator,
                # because it loops through all the operators in the ops dictionary
                break        
            
                    
        condition = ''.join(str(x) for x in list_condition) # re-construct the initial condition
        print(condition)


    for op_key in ops.keys():
        if notcheck:
            op_key = new_op # set the operator where the condition will be split
            splt=condition.split(new_op)
        else:
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
