

def _call_agg(table, row, aggtype):
    '''
    returns a Table object with only one row and column containing the result value
    of the aggregate function on the given table's row

    e.g.
    let the following query:

    'select max(salary) from workers'

    will call the following:

    '_call(workers, salary, _max)'
    
    which returns the following:

        max_salary
        9000
    '''
    from table import Table
    # helping dict
    agg_funs = {'min':_min,
                'max':_max,
                'avg':_avg,
                'count':_count,
                'sum':_sum}

    distinct = False
    col = row[0]

    if row[0] == "distinct":
        distinct = True
        col = row[1]

    s_table =  table._select_where(return_columns = col)

    raw_data  = []

    for d in s_table.data:
        raw_data.append(d[0])

    if distinct:
        c_names = ["agg_" + aggtype +"_distinct_" + col.replace(' ', '_')]
    else:
        c_names = ["agg_" + aggtype +"_" + col.replace(' ', '_')]
    c_types = s_table.column_types
    n_table = Table("temp", c_names, c_types)
    n_table.data = [[agg_funs[aggtype](raw_data,distinct)]]

    return n_table


def _call_agg_on_group_by(group_by_table, group_by_list, orignal_table, target_col, agg_fun_type):

    # first get the names of the 'groups'
    temp_table = group_by_table._select_where(return_columns=group_by_list)

    group_names  = []

    for d in temp_table.data:
        group_names.append(d[0])

    new_column =[]

    for group in group_names:

        condition = group_by_list + "=" + group

        tg = target_col[0]
        if len(target_col)==2:
            tg = target_col[1]

        agg_table = orignal_table._select_where(return_columns=tg, condition=condition )

        agg2 = _call_agg(agg_table,target_col,agg_fun_type)

        new_column.append(agg2.data[0][0])

    group_by_table.column_names.append(agg2.column_names[0])
    group_by_table.column_types.append(agg2.column_types[0])

    for i in range(len(group_by_table.data)):
        (group_by_table.data[i]).append(new_column[i])

    return group_by_table


def _max(rows,distinct):
    return max(rows)

def _min(rows,distinct):
    return min(rows)

def _sum(rows,distinct):
    if(distinct):
        rows = list(dict.fromkeys(rows)) # remove duplicates
    return sum(rows)

def _count(rows,distinct):
    if(distinct):
        rows = list(dict.fromkeys(rows)) # remove duplicates
    return len(rows)

def _avg(rows,distinct):
    if(distinct):
        rows = list(dict.fromkeys(rows)) # remove duplicates
    return sum(rows) / len(rows)
