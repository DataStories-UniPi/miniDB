import operator

def between(value,range):
    '''implements between functionality
    checks if value is between range (limits included)
     value: the specific value stored in table we are comparing
     range: range of accepted values from between keyword; is string; must contain split_key''' 

    split_key='&' # exp: BETWEEN 5 AND 25;
    if(split_key not in range):
        raise IndexError('Between syntax: BETWEEN "value1 & value2".')    
    try: # comparing floats-ints
        range = [float(x) for x in range.split(split_key)] # splits the between range
        float(value) # will work if value we are comparing is float or int
    except ValueError: # are we comparing strings?
        range = range.split('&') # range input must not include the split character
        #print("range:",range[0],range[1]) #DEBUG
    if ((value>=range[0] and value<=range[1]) or (value>=range[1] and value<=range[0])): # BETWEEN 5 & 10 == BETWEEN 10 & 5
        return True
    else: 
        return False

def not_between(value,range):
    '''reverse of between, is true when value is outside of range, limits exlcuded (like typical sql)''' 
    split_key='&' # exp: BETWEEN 5 AND 25;
    if(split_key not in range):
        raise IndexError('Between syntax: BETWEEN "value1 & value2".')    
    try: # comparing floats-ints
        range = [float(x) for x in range.split(split_key)] # splits the between range
        float(value) # will work if value we are comparing is float or int
    except ValueError: # are we comparing strings?
        range = range.split('&') # range input must not include the split character
        #print("range:",range[0],range[1]) #DEBUG
    if (not((value>=range[0] and value<=range[1]) or (value>=range[1] and value<=range[0]))): # BETWEEN 5 & 10 == BETWEEN 10 & 5
        return True
    else: 
        return False
    
def reverse_operator(op):
    '''reverses the operator when we are using NOT in specific condition, works with between and != : = too!'''
    return  {
    '>' : '<=',
    '>=' : '<',
    '<' : '>=',
    '<=' : '>',
    '!=' : '=',
    '=' : '!=',
    'between' : 'not_between'}.get(op) # specifically not adding not_between : between as it is will be the same as NOT BETWEEN
        
def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '!=': operator.ne,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                'between': between, # matching between keyword with def between(value,range)
                'not_between' : not_between} # matching not_between keyword with def not_between(value,range)

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    isNOT = False
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '!=': operator.ne,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between': between # added between operation
           }

    for op_key in ops.keys():
        if("not" in condition):
            condition = condition.replace("not ","") # delete NOT, save detection with bool isNOT and continue 
            isNOT =True # Not detected = True
            #print (condition,isNOT) #debug
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()
            if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')
            elif ' ' in right: # If it has whitespaces but no leading and trailing double quotes, throw.
                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')
            if(isNOT): # if not is detected then reverse the operator logic
                op_key = reverse_operator(op_key)
                #print("reversed")#debug
                #print("OPKEY",op_key)#debug
            #print(op_key)#debug
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
        '!=' : '!=',
        '=' : '='
    }.get(op)

