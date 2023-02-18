import operator
import re
from collections.abc import Iterable

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                
                #ADDED
                '!=': operator.ne,
                
                '=': operator.eq}

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False
    
#ADDED
def priority(op):
    if op == 'NOT' or op == 'not':
        return 3
    elif op == 'AND' or op == 'and' or op == 'OR' or op == 'or':
        return 2
    elif op != "(":
        return 4
    return 0
 
def infixToPrefix(condition, ops):
    operators = []
    operands = []
    infix = condition.split()
    for i in range(len(infix)):
        infix[i] = infix[i].strip()
        if (infix[i] == '('):
            operators.append(infix[i])
        elif (infix[i] == ')'):
            while (len(operators)!=0 and operators[-1] != '('):
                op = operators[-1]
                operators.pop()
                if op != "not" and op != "NOT":                
                    op1 = operands[-1]
                    operands.pop()
                    op2 = operands[-1]
                    operands.pop()
                    tmp = [op, op2 ,op1]
                else:
                    op1 = operands[-1]
                    operands.pop()
                    tmp = [op, op1]
                operands.append(tmp)
            operators.pop()
        elif (not infix[i] in ops):
            operands.append(infix[i] + "")
        else:
            while (len(operators)!=0 and priority(infix[i]) <= priority(operators[-1])):
                op = operators[-1]
                operators.pop()
                if op != "not" and op != "NOT":                
                    op1 = operands[-1]
                    operands.pop()
                    op2 = operands[-1]
                    operands.pop()
                    tmp = [op, op2 ,op1]
                else:
                    op1 = operands[-1]
                    operands.pop()
                    tmp = [op, op1]
                operands.append(tmp)
            operators.append(infix[i])
    while (len(operators)!=0):
        op = operators[-1]
        operators.pop()
        if op != "not" and op != "NOT":                
            op1 = operands[-1]
            operands.pop()
            op2 = operands[-1]
            operands.pop()
            tmp = [op, op2 ,op1]
        else:
            op1 = operands[-1]
            operands.pop()
            tmp = [op, op1]
        operands.append(tmp)
    return operands[-1]

def prefixToInfix(condition, ops):
    prefix = condition.split()
    stack = []
    i = len(prefix) - 1
    while i >= 0:
        if prefix[i] not in ops and prefix[i] not in [')', '(']:
            stack.append(prefix[i])
            i -= 1
        else:
            if prefix[i] == 'not' or prefix[i] == 'NOT':
                str = "(" + prefix[i] +  " " + stack.pop() + ")"
            else:
                str = "(" + stack.pop() + " " + prefix[i] + " " + stack.pop() + ")"
            stack.append(str)
            i -= 1
    return stack.pop()

def flatten(xs):
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x
 
def split_condition(condition):
    ops = {#ADDED
           'BETWEEN': None,
           'between': None,
           'OR': operator.or_,
           'or': operator.or_,
           'AND': operator.and_,
           'and': operator.and_,
           
           
           '>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           
           #ADDED
           '!=': operator.ne,
           
           '>': operator.gt,
           '<': operator.lt,
           
           #ADDED
           'not': operator.not_,
           'NOT': operator.not_,
           }
    
    #ADDED
#     print('condition', condition)
    splt = condition.split()
    for i in range(len(splt)):
        if splt[i] == 'between':
            splt[i] = ' >= '
            splt[i+3] = splt[i-1] + " <= " + splt[i+3]
    condition = " ".join(splt)
#     print('condition2', condition)
    words = re.split('( BETWEEN | between | OR | or | AND | and |>=|<=|=|!=|>|<| not | NOT )', condition)
#     print(words)
    condition = " ".join(words)
    prefix = infixToPrefix(condition, ops.keys())
#     print('condition', condition)
#     print('prefix', prefix)
    
    left = prefix[1]
    if isinstance(left, list):
        left = " ".join(flatten(left))
#     print('left',left)
#     print(prefixToInfix(left, ops.keys()))
    if len(prefix) > 2:
        right = prefix[2]
        if isinstance(right, list):
            right = " ".join(flatten(right))
    else:
        right = None
        
    op_key = prefix[0]
    if op_key == 'not' or op_key == 'NOT':
        op_key2 = prefix[1][0]
        left2 = prefix[1][1]
        if isinstance(left2, list):
            left2 = " ".join(flatten(left2))
#         print('left2',left2)
#         print(prefixToInfix(left2, ops.keys()))
        if len(prefix[1]) > 2:
            right2 = prefix[1][2]
            if isinstance(right2, list):
                right2 = " ".join(flatten(right2))
#             print('right2',right2)
        else:
            right2 = None
        if op_key2 == 'NOT' or op_key2 == 'not':
            condition = prefixToInfix(left2, ops.keys()).removeprefix('(').removesuffix(')')
            print(condition)
            return split_condition(condition)
        if op_key2 == 'OR' or op_key2 == 'or':
            return split_condition("not ( " + prefixToInfix(left2, ops.keys()) + " ) and not ( " + prefixToInfix(right2, ops.keys()) + " )")
        if op_key2 == 'AND' or op_key2 == 'and':
            condition = "not " + prefixToInfix(left2, ops.keys()) + " or not " + prefixToInfix(right2, ops.keys())
#             print(condition)
            return split_condition(condition)
        if op_key2 == '>=':
            return left2.removeprefix('('), '<', right2.removesuffix(')')
        if op_key2 == '<=':
            return left2.removeprefix('('), '>', right2.removesuffix(')')
        if op_key2 == '=':
            return left2.removeprefix('('), '!=', right2.removesuffix(')')
        if op_key2 == '>':
            return left2.removeprefix('('), '<=', right2.removesuffix(')')
        if op_key2 == '<':
            return left2.removeprefix('('), '>=', right2.removesuffix(')')
    #ADDED
    if ops[op_key] == operator.and_:            
        return left, op_key, right
    if ops[op_key] == operator.or_:
        return left, op_key, right
    if ops[op_key] == None:
        left, right = splt[0].strip(), splt[1].strip()
        return left, op_key, right
#     print(left, right)
    
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
