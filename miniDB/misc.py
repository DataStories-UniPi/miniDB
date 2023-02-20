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
            'and': operator.and_,
            'or': operator.or_,
            'not': operator.not_}
    try:        
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False
    
def split_condition(condition):
    ops = { 
            ' or ': operator.or_,
            ' and ': operator.and_,                                 
            ' not ': operator.not_,
            ' between ': None,
#            ' & ': None,
            '>=': operator.ge,
            '<=': operator.le,
            '=': operator.eq,
            '>': operator.gt,
            '<': operator.lt                      
           }            
    condition = " "+condition        
    for op_key in ops.keys():
        splt=condition.split(op_key)               
        if op_key == " not " and " not " in condition:            
            l, o, r = split_condition(splt[1])            
            return l, o, r
        elif len(splt)>1:            
            if op_key == " or " or  op_key == " and " :                      
                left = []
                op_k = []
                right = []
                for i in range(len(splt)):                    
                    x, y, z = split_condition(splt[i])
                    if not isinstance(x, list) :
                        x, z = x.strip(), z.strip()                       
                        if z[0] == '"' == z[-1]: # If the value has leading and trailing quotes, remove them.
                            z = z.strip('"')
                        elif ' ' in z: # If it has whitespaces but no leading and trailing double quotes, throw.
                            raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')
                        if z.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                            raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')
                        left.append(x)
                        op_k.append(y)
                        right.append(z)
                    else:
                        for j in range(len(x)):
                            x[j], z[j] = x[j].strip(), z[j].strip()                       
                            if (z[j])[0] == '"' == (z[j])[-1]: # If the value has leading and trailing quotes, remove them.
                                z[j] = z[j].strip('"')
                            elif ' ' in z[j]: # If it has whitespaces but no leading and trailing double quotes, throw.
                                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')
                            if z[j].find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')
                            left.append(x[j])
                            op_k.append(y[j])
                            right.append(z[j])
                return left, op_k, right                                   
            elif op_key == " between " and " between " in condition:
                left = []
                op_k = []
                right = []                
                temp = splt[1].split(" & ")                    
                if int(temp[0]) < int(temp[1]):                    
                    left.append(splt[0].strip())
                    op_k.append('>')
                    right.append(temp[0])
                    left.append(splt[0].strip())
                    op_k.append('<')
                    right.append(temp[1])
                elif int(temp[0]) > int(temp[1]):                                        
                    left.append(splt[0].strip())
                    op_k.append('>')
                    right.append(temp[1])
                    left.append(splt[0].strip())
                    op_k.append('<')
                    right.append(temp[0])                
                return left, op_k, right 
            else:    
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
