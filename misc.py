import operator
import re

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
           '<': operator.lt,
           '>=': operator.ge,
           '<=': operator.le,
           '==': operator.eq,
           # the 3 operators added with "," before
           # and after to avoid conflicts with other characteristics of the condition
           ',between,': between,
           ',in,': in_op,
           ',like,': like}

    try:
        if op == ',between,':
            # if the operator is "between" the b will be for example "100 and 200" so ...
            m_splt = b.split('and')
            if len(m_splt) > 1:
                return ops[op](a, type(a)(m_splt[0]), type(a)(m_splt[1]))
        return ops[op](a, b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    condition = condition.replace(' ','') # remove all whitespaces
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '==': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           ',between,': between,
           ',in,': in_op,
           ',like,': like}

    for op_key in ops.keys():
        splt = condition.split(op_key)
        if len(splt) > 1:
            return splt[0], op_key, splt[1]

def between(a, b, c):
    '''
    b <= a <= c
    '''
    try:
        if c is not None and b < c:
            return operator.ge(a, b) and operator.le(a, c)
    except TypeError:
        return False

def in_op(a, b):
    '''
    a should be in b list
    '''
    try:
        if type(b) == list:
            for i in b:
                if i == a:
                    return True
            return False
    except TypeError:
        return False

def like(a, b):
    try:
        b = b.replace('%', '.*')
        b = b.replace('_', '.')
        b += '$'
        return bool(re.search(b, a))
    except TypeError:
        return False

'''
Examples for between, in, like operators for vsmdb:
    - between:
        db.select('classroom', '*', "capacity,between,10and30")
        
        Output:
        ## classroom ##
        building (str)      room_number (str)    capacity (int)
        ----------------  -------------------  ----------------
        Painter                           514                10
        Watson                            100                30
    
    - in:
        db.select('classroom', '*', "building,in,['Painter']")
        
        Output:
        ## classroom ##
        building (str)      room_number (str)    capacity (int)
        ----------------  -------------------  ----------------
        Painter                           514                10
    
    - like:
        db.select('classroom', '*', "building,like,P%")
        
        Output:
        ## classroom ##
        building (str)      room_number (str)    capacity (int)
        ----------------  -------------------  ----------------
        Packard                           101               500
        Painter                           514                10
'''