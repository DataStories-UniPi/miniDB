import operator

def get_op(op, x, y):
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                '!=': operator.ne}  
    
    try:   
         if ((op == 'between' or op == 'not_between') and type(y) != list ):
            raise ValueError(f'Error: \nParameter Value for condition with operator between doesn\'t include range.')

        if (op == 'between'):  
            return ( x>y[0] and x<y[1])
        elif (op == 'not_between'): 
            smaller = float(x)<float(y[0])
            bigger = float(x)>float(y[1])
            return smaller or bigger
        return ops[op](x,y)

    except TypeError:  
        return False 

        def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'between': None} 

           condition.strip()
    has_NOT_operator = False
    if (condition.startswith("not ")):
        has_NOT_operator = True
        condition = condition[3:]    

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)==2:
            left, right = splt[0].strip(), splt[1].strip()

            if right[0] == '"' == right[-1]:
                right = right.strip('"')
            elif ' ' in right and op_key!='between': 
                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')
            if right.find('"') != -1: 
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

            if (op_key == 'between'):  

                if (right.find('(') == -1 or right.find(')') == -1):
                    raise ValueError(f'Invalid condition: {condition}\nRange not specified after BETWEEN operator.\nexample: (1,2).')

                right = right.strip('(') 
                right = right.strip(')')

                if (right.find('(') != -1 or right.find(')') != -1): 
                    raise ValueError(f'Invalid condition: {condition}\nWrong use of Parenthesis marks inside the value.')
                Rightsplt = right.split(',') 
                if len(Rightsplt)==2:
                    right = [Rightsplt[0].strip(),   
                            Rightsplt[1].strip()]                      
                else:
                    raise ValueError(f'Invalid condition: {condition}\nWrong input inside the range of BETWEEN operator.')

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

    split_condition = [] 
    pointerA = 0 
    pointerB = 0 

    while pointerB+5 < len(condition): 
        if (condition[pointerB:(pointerB+5)] == ' and '):      
            split_condition.append(condition[pointerA:pointerB]) 
            split_condition.append('and') 
            pointerA = pointerB+5 
            pointerB = pointerB+4
        elif (condition[pointerB:(pointerB+4)] == ' or '):
            split_condition.append(condition[pointerA:pointerB]) 
            split_condition.append('or') 
            pointerA = pointerB+4 
            pointerB = pointerB+3
        pointerB = pointerB +1 

    split_condition.append(condition[pointerA:].replace('\n',''))

    return split_condition

def high_priority_conditions(condition):

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
         left_tables = []
    right_tables = []
     
     if isinstance(dict['left'],str):
        left_tables.append(dict['left']) 
    else: 
        for l in return_tables_in_join_dict(dict['left']): 
            for j in l: 
                left_tables.append(j in x) 

    if isinstance(dict['right'],str):
        right_tables.append(dict['right'])
    else:
        for x in return_tables_in_join_dict(dict['right']):
            for j in i:
                right_tables.append(j in l)
    
    return left_tables,right_tables
    