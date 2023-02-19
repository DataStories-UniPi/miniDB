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
                '!=': operator.ne}

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def replace_char(string, index, char):
    string = list(string)
    string[index] = char
    return ''.join(string)



def split_bettwen_con(condition):

    print(condition)
    #when you find the word BETWEEN you split the condition in 2 parts
    #the first part is the column name
    betpos = condition.find('between')
    column = condition[:betpos].strip()
    #find the index of the first and the last number
    small = condition.find(' ', betpos)
    big = condition.find(' ', small+2)
    small = condition[small:big].strip()
    big = condition[big:].strip()
    big= big[big.find(' '):]
    print(column,small,big)

    return column, small, big



def split_condition(condition):
    ops = {'>=': operator.ge,
           '!=': operator.ne,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           }

    while 'not' in condition:
        # check if not is in the string and if it is change the operator to the opposite
        if 'not' in condition:
            notpos = condition.find('not')
            #search the next not  space characer after the notpos
            nexpos=notpos+3
            while condition[nexpos] == ' ':
                nexpos += 1
           

            condition = condition.replace('not', '', 1)
            eqop= False
           # print(condition+"hm")
            tmpcon = condition[notpos:]
            #find the index of the first  ops in the tmpcon
            opindex= 100000000000
            for op in ops:
                if op in tmpcon and tmpcon.find(op) <opindex:
                    opindex = tmpcon.find(op)

            for op in ops:

                if op in tmpcon and tmpcon.find(op) == opindex:
                    #print(condition[notpos:])
                    op_pos = tmpcon.find(op)  + len(condition[:notpos])


                    if condition[op_pos+1] == '=' :
                        apop=condition[op_pos]+'='
                        condition= replace_char(condition, op_pos+1, ' ')



                    else:
                        apop=condition[op_pos]


                    condition  =replace_char(condition, op_pos, not_op(apop))


                    break
            #take a word from a string by the position of first letter
        # remove the the first NOT from the string

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()


            if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')
            elif ' ' in right: # call again spilt_condition with the right side of the condition
                return   left, op_key, right
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
def not_op(op):
    '''
    Get the opposite operator
    '''
    return {
        '>' : '<=',
        '>=' : '<',
        '<' : '>=',
        '<=' : '>',
        '=' : '!=',
        '!=':'='
    }.get(op)