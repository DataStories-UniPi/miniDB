import operator
import re #for regural expressions in like operator
import sys


def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
           '$not_in$': not operator.contains,
           '$in$': operator.contains,
           '$not_like$': 'not like',
           '$like$': 'like',
           '$not_between$':'not between',
           '$between$': 'between'}

    try:
        if op in ['$in$', '$not_in$']:
            values = b.replace('(', '').replace(')', '').split(',')
            if op == '$in$':
                return a in values
            else:
                return a not in values
        elif op in ['$like$', '$not_like$']:
            try:
                regex = b.replace('%', '[a-zA-Z0-9]*').replace('_', '[a-zA-Z0-9]')
                regex = re.compile(regex)
                found = bool(regex.match(a))
                return found if op == '$like$' else not found
            except AttributeError:
                print('Like operator can only be used with Strings')
                sys.exit(2)
        elif op in ['$not_between$','$between$']:
            try:
                v = b.split('and')
                min, max = float(v[0]),float(v[1])
                if min > max :
                    print('Bad arguments for Between operator')
                    sys.exit(3)
                return min <= a <= max if op == '$between$' else a < min or a > max
            except TypeError:
                print('Between is only supported for numeric columns')
                sys.exit(2)
            except ValueError:
                print('Bad use of Between: \n\t Example: SELECT * FROM classroom WHERE capacity BETWEEN 1 AND 100')
                sys.exit(4)
        else:
            return ops[op](a, b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    condition=condition.replace(' in ', ' $in$ ').replace(' like ', ' $like$ ')\
        .replace('not $in$', '$not_in$').replace('not $like$', '$not_like$')\
        .replace(' between ', ' $between$ ').replace('not $between$', '$not_between$')
    condition = condition.replace(' ','') # remove all whitespaces
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt,
           '$not_in$': not operator.contains,
           '$in$': operator.contains,
           '$not_like$': 'not like',
           '$like$': 'like',
           '$not_between$': 'not between',
           '$between$': 'between'}

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            return splt[0], op_key, splt[1]
