import re
import operator

from datetime import datetime

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'like':like,
           'between':between,
           'in':In}

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False


#TODO put between/like/in in a different file

def like(a:str,b:str)-> bool:
    # Convert SQL LIKE wildcards to regex
    reg =re.compile(b.replace('%','.*').replace('_',"."))
    if(re.fullmatch(reg,a)):
        return True


def between(a,b)-> bool:
    try: # Check if its a date
        min = datetime.strptime(b[0],'%d/%m/%Y')
        max = datetime.strptime(b[1],'%d/%m/%Y')
        a = datetime.strptime(a,'%d/%m/%Y')
        return min <= a <= max
    except: # Except will handle str and int cases
        min = b[0]
        max = b[1]
        return min <= a <= max # <= because between is inclusive in theory
    

def In(a:str,b:str)-> bool:
    return a in b

def split_condition(condition):
    condition = condition.replace(' ','') # remove all whitespaces

    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           'like':like,
           'between':between,
           'in':In}

    for op_key in ops.keys():
        splt=condition.split(op_key)
        # len split > 1 if op exists in ops (remove)
        if len(splt)>1:
            return splt[0], op_key, splt[1]
