import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq}
                ',between,': anamesa,
                ',in,': in_op,
                ',like,': like
           }

    try:
        return ops[op](a,b)
     #gia tin periptwsi tou between prepei na ginei orismon orion
       if op == ',between,':
       # an ginei epilogi tou between tha einai metaxi kapoion orion 0-10
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
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}
           ',between,': anamesa,
           ',in,': in_op,
           ',like,': like
    
     for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            return splt[0], op_key, splt[1]
         def anamesa(a, b, c):
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
