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
        # checking the condition if op is Between
        if ((op == 'between' or op == 'not_between') and type(b) != list ):
            raise ValueError(f'Error: \nParameter Value for condition with operator between doesn\'t include range.')

        if (op == 'between'):   # checking the condition if op is Between
            return ( a>b[0] and a<b[1])
        elif (op == 'not_between'): # checking the condition if op is Between with NOT operator in front of the condition
            smaller = float(a)<float(b[0])
            bigger = float(a)>float(b[1])
            return smaller or bigger

        #   for any other operator do this
        return ops[op](a,b)

    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False
        

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between': None} # added BETWEEN operator
    # if op is BETWEEN then right will be (value1,value2). We return both values
    

    # check for NOT operator
    condition.strip()
    has_NOT_operator = False
    if (condition.startswith("not ")):
        has_NOT_operator = True
        condition = condition[3:]    


    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)==2:
            left, right = splt[0].strip(), splt[1].strip()

            if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')
            elif ' ' in right and op_key!='between': # If it has whitespaces but no leading and trailing double quotes, throw. 
                # this doesnt apply to BETWEEN operator
                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')



            if (op_key == 'between'):   # if op is between 
                # if operator is between, then right should not be one value, but a list with 2 values representing the range

                if (right.find('(') == -1 or right.find(')') == -1): # check if range is defined in parenthesis
                    raise ValueError(f'Invalid condition: {condition}\nRange not specified after BETWEEN operator.\nexample: (1,2).')

                right = right.strip('(') # strip '(' and ')'
                right = right.strip(')')

                if (right.find('(') != -1 or right.find(')') != -1): # If there are any parenthesis in the value, throw. (Notice we've already removed the leading and trailing ones)
                    raise ValueError(f'Invalid condition: {condition}\nWrong use of Parenthesis marks inside the value.')
                

                Rightsplt = right.split(',') # split current right
                if len(Rightsplt)==2:
                    right = [Rightsplt[0].strip(),   # make right a list with the 2 values of the range 
                            Rightsplt[1].strip()]                      
                else:
                    raise ValueError(f'Invalid condition: {condition}\nWrong input inside the range of BETWEEN operator.') # if there is no comma or more than 1 then give error
                



            # if there was NOT, change the operator to the oposite one
            if (has_NOT_operator):
                reverse_ops = {'>': '<=',
                    '<': '>=',
                    '>=': '<',
                    '<=': '>',
                    '=': '!=',
                    'between': 'not_between'} 
                op_key = reverse_ops.get(op_key)




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




def split_complex_conditions(condition):
    '''
        Gets a complex condition and splits it into simple ones.
        Returns a list with the conditions, 'and' and 'or' operators in the correct order

        example:
        condition: a>b and c>d or e>f
        list:      ['a>b', 'and', 'c>d', 'or', 'e>f']
    
    '''
    split_condition = [] # list with all conditions and all operators in correct order
    pointerA = 0 # shows the beginning of the condition that we'll add to the list split_condition
    pointerB = 0 # (eventually) shows the end of the condition that we'll add to the list split_condition

    while pointerB+5 < len(condition): # pointerB+5 shouldn't go beyond the length of the condition or else we'll go out of bounds 
        if (condition[pointerB:(pointerB+5)] == ' and '): # checking if pointerB is pointing to 'AND' operator
            # we reached to the end of the first condition, which is connected with AND to the next one
            split_condition.append(condition[pointerA:pointerB]) # adding the condition to split_condition
            split_condition.append('and') # adding the operator to split_condition
            pointerA = pointerB+5 # pointing pointerA to the beginning next condition
            pointerB = pointerB+4

        elif (condition[pointerB:(pointerB+4)] == ' or '): # checking if pointerB is pointing to 'OR' operator
            # we reached to the end of the first condition, which is connected with OR to the next one
            split_condition.append(condition[pointerA:pointerB]) # adding the condition to split_condition
            split_condition.append('or') # adding the operator to split_condition
            pointerA = pointerB+4 # pointing pointerA to the beginning next condition
            pointerB = pointerB+3


        pointerB = pointerB +1 # pointing B to the next letter

    split_condition.append(condition[pointerA:].replace('\n','')) # adding the last condition (which is not added in the loop bcs there is no 'AND' or 'OR' in the end)

    return split_condition


def high_priority_conditions(condition):
    '''
    Input: Gets a condition that has been split before (by using the function split_complex_condition)
    Output: Returns a list with the conditions that have high priority the in the whole condition.
    In other words, returns the conditions that have to be true in order for the whole condition to be true
    example:
        condition:  ['a>b', 'or', 'c>d', 'and', 'e>f']
        return: ['c>d', 'e>f']
    '''
    high_priority_conditions = []
    
    if 'or' not in condition:
        for i in condition:
            if i != 'and':
                high_priority_conditions.append(i) 
    
    else:
        i = len(condition)-1
        while condition[i-1] != 'or':
            if condition[i] != 'and':
                high_priority_conditions.append(i)
            i = i - 1
    
    return high_priority_conditions

        


def return_tables_in_join_dict(dict):
    '''
        Is used in Query_optimizer to find equivalent queries
        Gets a dict for join query and returns all tables that are joined to make the final table
        (left or right column might be dict that joins other columns)
    '''
    left_tables = []
    right_tables = []

    if isinstance(dict['left'],str): # if left is string then its table
        left_tables.append(dict['left']) # add it to the list
    else: # else left is dict that joins 2 other tables
        for x in return_tables_in_join_dict(dict['left']): # for tables joined in left
            for j in x: # for each table inside them
                left_tables.append(j in x) # add it in the list

    # do the same for right table 
    if isinstance(dict['right'],str):
        right_tables.append(dict['right'])
    else:
        for x in return_tables_in_join_dict(dict['right']):
            for j in x:
                right_tables.append(j in x)
    
    return left_tables,right_tables

