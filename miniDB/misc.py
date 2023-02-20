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

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
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

def convert_to_RA(dic):
    '''
    Convert the given query to relational algebra string
    '''
    RA_dic = convert_query_dic_to_RA_dic(dic)
    projection = RA_dic['projection']
    selection = selection_to_string(RA_dic['selection'])
    table = table_name_to_string(RA_dic['table'])
    RA_expression = ''
    if RA_dic['distinct']:
        RA_expression += "δ \n "
    if projection != '*':
        RA_expression += "Π " + projection + "\n  "
    RA_expression += "σ " + selection + " (" + table +")"
    return RA_expression

def convert_query_dic_to_RA_dic(dic):
    '''
    Convert a given query dictionary to a relational algebra dictionary
    '''
    RA_expression = {
        'distinct': None,
        'projection': 'select',
        'selection': 'where',
        'table': 'from'
    }
    RA_expression['projection'] = dic['select']
    RA_expression['selection'] = dic['where']
    RA_expression['table'] = simplify_from(dic['from'])
    print(RA_expression['table'])
    if dic['distinct'] is not None:
        RA_expression['distinct'] = True
    return RA_expression

def simplify_from(condition):
    if (isinstance(condition, dict)) and (isinstance(condition['right'], dict)):
        condition['right'] = simplify_from(condition['right']['from'])
        return condition
    elif (isinstance(condition, dict)) and (not (isinstance(condition['right'], dict))):
        return condition
    else:
        return ''.join(condition)

def evaluate_selection(condition):
    if (isinstance(condition, dict)) and isinstance(condition['right'], dict):
        condition['right'] = evaluate_selection(condition['right'])
        return condition
    elif isinstance(condition, dict):
        return condition
    else:
        table_string = condition
        return table_string

def selection_to_string(condition):
    if isinstance(condition, dict):
        if (condition['left'] == None):
            temp_string = condition['operator'] + '(' + selection_to_string(condition['right']) + ')'
        else:
            temp_string = selection_to_string(condition['left']) + ' ' + condition['operator'] + ' ' + selection_to_string(condition['right'])
        return temp_string
    else:
        return ''.join(condition)

def table_name_to_string(table):

    if (isinstance(table, dict)) and (table['join'] is not None):
        join_character = " ⋈ "
        if table['join'] == 'left':
            join_character = " ⋈ L "
        elif table['join'] == 'right':
            join_character = " ⋈ R "
        elif table['join'] == 'full':
            join_character = " ⋈ o "

    if (isinstance(table, dict)):
        table_string = table_name_to_string(table['left']) + join_character + table['on'] +' '+ table_name_to_string(table['right'])
        return table_string
    else:
        table_string = table
        return ''.join(table_string)
