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
                ',between,': anamesa_se_duo_times,
                ',in,': function_in,
                ',like,': function_like
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
           ',between,': anamesa_se_duo_times,
           ',in,': function_in,
           ',like,': function_like}
    
     for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            return splt[0], op_key, splt[1]
        
#synartisi pou pernei san orisma dio times min, max kai epistrefei oles tis times metaxi auton
def anamesa_se_duo_times(a_result, b_min, c_max):

#an to c_max den einai keno kai  to b_min einai mikrotero apo to c_max epistrefei oles tis times metaxi ton timon bmin < a_result<c_Max
#an kati paei lathos gurnaei flase kai error

    try:
        if c_max is not None and b_min < c_max:
           return operator.ge(a_result, b_min) and operator.le(a_result, c_max)
    except TypeError:
           return False

#synartisi pou pernei san orisma to zitoumeno kai mia lista me strings       
def function_in(zitomeno, lista):
#to zitoumeno prepei na iparxei stin lista
    try:
        if type(lista) == list:
           for i in lista:
              if i == zitomeno:
                 return True
           return False
    except TypeError:
           return False
# sunartisi pou pernei san orisma ena zitoumeno kai mia lista apo strings kai ginete elenxo
#me xrisi ton kanonikwn ekfrasewn antikathistonas  "%" me ".*" kai to "_" me "."

def function_like(zitomeno, lista):
     try:
         lista = lista.replace('%', '.*')
         lista = lista.replace('_', '.')
         lista += '$'
         return bool(re.search(lista, zitomeno))
     except TypeError:
            return False
