import operator

'''
    Returns true if the query condition could result in multiple records,
    false if the result is at most one record.
    For example: 
    id > 12 -> true
    id = 12 -> false
    (assuming column id is unique)
    
    Args:
        condition_tokens: the list of tokens that the condition consists of
        condition_tokens: [ 'column_name', 'operator', 'value' ]
'''
def is_range_query(condition_tokens):
    if len(condition_tokens) > 1:
        return condition_tokens[1] != '='   # if the operator is anything other than '=' return true


'''
    Creates and returns a condition plan (dictionary) based on the given condition, 
    by recursively expanding a dictionary
    
    Args: 
        condition: a string (the part of the query to the right of keyword 'where')
    
    Examples:
        example0:
        condition: 'id > 3 and name = john'
        result: { 'id > 3: [ id, '>', 3, 'and' ], { 'name = john': [ name, '=', 'john' , ''] } }
        
        example1:
        condition: 5 > 3 and 50 between 40 and 60 or 20 between 80 and 100 
        result: { '5 > 3' : [ '5', '>', '3', 'and', { '50 between 40 and 60' : [ '50', 'between', '40', 'and', '60', 'or', { '20 between 80 and 100': [ '20', 'between', '80', 'and', '100', '' ] } ] } ] }
        
    The condition_plan consists of:
    { query: [ column_name/value, operator, column_name/value, keyword_operator(and/or/ect) ], empty string or another dictionary of the same form }
    where keyword_operator is the keyword operator with which the left condtion connects with the right condition
'''
def get_condition_plan(condition, join=False):     
    if join:
        return get_condition_plan(condition)
    
    kw_ops = [ 'between', 'and', 'or', 'not' ]
    l_substr, op, r_substr = '', '', ''
    
    condition = condition.replace('\"', '')    
    condition = condition.strip()

    for word in condition.split(' '):
        if word in kw_ops:
            indx = condition.find(' ' + word + ' ')
            if indx != -1:
                if word == 'between':   # case where word is 'between' must be handled differently
                    tmp = condition[ indx + len('between') + 1 :].split(' ')
                    l_substr = condition[:indx] + ' between' + ' '.join(tmp[:4])
                    l_substr = l_substr.strip(' ')
                    if len(tmp) > 4:
                        r_substr = ' '.join(tmp[5:])
                        op = tmp[4]
                else:                
                    l_substr = condition[:indx]
                    r_substr = condition[indx + len(word) + 1 :]                
                    op = word

                if r_substr != '':
                    if word == 'not':
                        r_substr = condition[:indx] + r_substr
                    if r_substr[0] == '(' and r_substr[-1] == ')':
                        r_substr = r_substr.strip('()')
                break
    dic = dict()
    l_substr = l_substr.strip(" '\"''\t")

    if l_substr == '':
        l_substr = condition
    tokens = l_substr.split(' ')
    dic[l_substr] = [ tokens[i] for i in range(len(tokens)) ]

    if op != 'between':
        dic[l_substr].append(op)

    if ' ' in r_substr:
        r_substr = r_substr.strip(" '\"''\t")
        dic[l_substr].append(get_condition_plan(r_substr))
    return dic



'''
    Recursively scans given condition_plan and returns all the column names in it as a set of strings
    
    Args:
        condition_plan: a condition plan generated from the get_condition_plan function above
        column_names: the set of unique column names that the condition_plan contains
'''
def get_column_names_from_condition_plan(condition_plan, column_names=set()):
    kw_ops = set(['between', 'and', 'or', 'not'])
    arithmetic_ops = set(['>=', '<=', '=', '!=', '>', '<'])
    
    k = list(condition_plan.keys())[0]
    for word in k.split():
        if word not in kw_ops and word not in arithmetic_ops and not word.isnumeric():      # if word is not an operator or a value its a column name, add it to column_names set
            column_names.add(word)
    if isinstance(condition_plan[k][-1], dict):     # if the last element in the list is a dictionary call get_column_names_from_condition_plan with the subdictionary as condition_plan
        return get_column_names_from_condition_plan(condition_plan[k][-1], column_names)
    return column_names     # if the last element in the list is not a dictionary the scan is over, return found column_names


'''
    Returns True if x is in the range [low, high], false otherwise
    
    Args:
        x: value in question, could be an arithmetic value or a string
        low: lower limit, could be an arithmetic value or a string
        high: upper limit, could be an arithmetic value or a string
        
'''
def between(x, low, high):
    return eval(x + '>=' + low) and eval(x + '<=' + high) # operator.ge(x, low) and operator.le(x, high)



'''
    Evaluates given condition, returns True or False
    
    Args:
        condtion: a condition plan like the one get_condition_plan function returns (type dictionary)
'''
def evaluate_condition(condition):
    '''
        Evaluates a basic condition
        Args:
            condition: a list or a tuple consisting of at most 5 elements
            condition: 
            [ operand, operator, operand ] or
            [ operand, 'between',  lower_limit, 'and', upper_limit] if the keyword operator is 'between'
    '''
    def evaluate_basic_condition(condition):
        ops = {
            '>=': operator.ge,
            '<=': operator.le,
            '=': operator.eq,
            '!=': operator.ne,
            '>': operator.gt,
            '<': operator.lt,
            'between': between
        }

        if isinstance(condition, (list, tuple)):
            if len(condition) == 3:
                if not condition[0].isnumeric():
                    return ops[condition[1]](condition[0], condition[2])
                if condition[1] == '=':
                    condition[1] = '=='
                return eval(condition[0] + condition[1] + condition[2])
            elif len(condition) == 5:   # has between
                return between(condition[0], condition[2], condition[4])
    
    k = list(condition.keys())[0]     # condition has only one key
    l = condition[k]

    if isinstance(l[-1], dict):
        right_part_result = evaluate_condition(l[-1])
        
        if l[-2] == 'and' and not right_part_result:
            return False
        
        left_part_result = evaluate_basic_condition(l[:-2])

        if l[-2] == 'or':
            return left_part_result or right_part_result
        elif l[-2] == 'and':
            return left_part_result and right_part_result
        elif l[-2] == 'not':
            return not right_part_result
    else:
        return evaluate_basic_condition(l[:-1])

    
#print(get_condition_plan('tot_cred >= 30 and tot_cred <= 60 or tot_cred >= 80 and tot_cred <= 100 and dept_name = "history"'))
#print(get_condition_plan('tot_cred > 80'))
#print(get_condition_plan('not id > 3'))
#print(get_condition_plan('id > 3 and (tot_cred between 40 and 60 or tot_cred between 80 and 100)'))
#condition_plan = get_condition_plan('4 > 3 and 12 not between 40 and 60 or 12 between 20 and 100')
#print(condition_plan)
#print(evaluate_condition(condition_plan))
#column_names_in_condition = get_column_names_from_condition_plan(condition_plan)
#print(column_names_in_condition)
#print(get_condition_plan('tot_cred between 40 and 60 or tot_cred between 80 and 100'))
#print( evaluate_condition( { '5 > 3' : [ '5', '>', '3', 'and', { '50 between 40 and 60' : [ '50', 'between', '40', 'and', '60', 'or', { '20 between 80 and 100': [ '20', 'between', '80', 'and', '100', '' ] } ] } ] } ) )
#print(get_condition_plan('word > a or id > 3'))
