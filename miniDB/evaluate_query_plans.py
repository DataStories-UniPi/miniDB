'''
def evaluate_query_plans(queries, stats, has_index):
    query_costs = []
    for query in queries:
        cost = 0
        # Check if the query is a simple SELECT
        if "JOIN" not in query:
            for column in stats:
                # Calculate cost based on number of distinct values
                cost += stats[column]["distinct_values"]
            query_costs.append(cost)
        else:
            # Check if there's an index in either of the joined tables
            index_found = False
            for table in [table.strip() for table in query.split("JOIN")[0:2]]:
                if has_index(table):
                    index_found = True
                    break
            # If there's an index, use index nested loop join
            if index_found:
                # Calculate cost based on size of smaller table
                join_tables = [table.strip() for table in query.split("JOIN")]
                smaller_table = min([table for table in join_tables if table in stats], key=lambda x: stats[x]["size"])
                cost = stats[smaller_table]["size"]
                query_costs.append(cost)
            else:
                # If no index, use block nested loop join
                join_tables = [table.strip() for table in query.split("JOIN")]
                cost = sum([stats[table]["size"] for table in join_tables if table in stats])
                query_costs.append(cost)
    return query_costs
'''
'''
def evaluate_query_plans(queries, stats, has_index):
    query_costs = {}
    for query in queries:
        cost = 0
        # Check if the query is a simple SELECT
        if "JOIN" not in query:
            for column in stats:
                # Calculate cost based on number of distinct values-This approach is just a common heuristic for estimating query costs
                cost += stats[column]["distinct_values"]
            query_costs[query] = cost
        else:
            # Check if there's an index in either of the joined tables
            index_found = False
            for table in [table.strip() for table in query.split("JOIN")[0:2]]:
                if has_index(table):
                    index_found = True
                    break
            # If there's an index, use index nested loop join
            if index_found:
                # Calculate cost based on size of smaller table
                join_tables = [table.strip() for table in query.split("JOIN")]
                smaller_table = min([table for table in join_tables if table in stats], key=lambda x: stats[x]["size"])
                cost = stats[smaller_table]["size"]
                query_costs[query] = cost
            else:
                # If no index, use block nested loop join
                join_tables = [table.strip() for table in query.split("JOIN")]
                cost = sum([stats[table]["size"] for table in join_tables if table in stats])
                query_costs[query] = cost
    return min(query_costs, key=query_costs.get)
'''
'''
def sqlify(query):
    sql = "SELECT "
    
    if query.get("distinct"):
        sql += "DISTINCT "
    
    select = query.get("select")
    if type(select) == str:
        sql += select + " "
    elif type(select) == dict:
        sql += sqlify(select) + " "
    
    from_clause = query.get("from")
    if type(from_clause) == str:
        sql += "FROM " + from_clause + " "
    elif type(from_clause) == dict:
        if from_clause.get("join"):
            sql += "FROM " + from_clause.get("left") + " "
            sql += from_clause.get("join").upper() + " JOIN "
            sql += from_clause.get("right") + " "
            on_clause = from_clause.get("on")
            if type(on_clause) == str:
                sql += "ON " + on_clause + " "
            elif type(on_clause) == dict:
                if on_clause.get("and"):
                    sql += "ON " + on_clause.get("left") + " AND " + on_clause.get("right") + " "
    
    where = query.get("where")
    if where:
        if type(where) == str:
            sql += "WHERE " + where + " "
        elif type(where) == dict:
            if where.get("and"):
                sql += "WHERE " + where.get("left") + " AND " + where.get("right") + " "
    
    order_by = query.get("order by")
    if order_by:
        sql += "ORDER BY " + order_by + " "
        if query.get("desc"):
            sql += "DESC "
    
    limit = query.get("limit")
    if limit:
        sql += "LIMIT " + str(limit)
    
    return sql
'''
'''
def sqlify(query):
    if isinstance(query, dict):
        keys = list(query.keys())
        select = query.get('select')
        from_ = query.get('from')
        where = query.get('where')
        distinct = query.get('distinct')
        order_by = query.get('order by')
        limit = query.get('limit')
        desc = query.get('desc')
        
        if 'join' in keys:
            join = query['join']
            left = query['left']
            right = query['right']
            on = query['on']
            
            if isinstance(on, dict) and 'and' in on.keys():
                and_ = on['and']
                left_cond = and_['left']
                right_cond = and_['right']
                
                return f"SELECT {select} FROM {left} {join} JOIN {right} ON {left_cond} AND {right_cond}"
            
            return f"SELECT {select} FROM {left} {join} JOIN {right} ON {on}"
        
        if isinstance(from_, dict):
            return sqlify(from_)
        
        if isinstance(where, dict) and 'and' in where.keys():
            and_ = where['and']
            left_cond = and_['left']
            right_cond = and_['right']
            
            return f"SELECT {select} FROM {from_} WHERE {left_cond} AND {right_cond}"
        
        if order_by:
            if desc:
                return f"SELECT {distinct} {select} FROM {from_} WHERE {where} ORDER BY {order_by} DESC"
            return f"SELECT {distinct} {select} FROM {from_} WHERE {where} ORDER BY {order_by}"
        
        if limit:
            return f"SELECT {distinct} {select} FROM {from_} WHERE {where} LIMIT {limit}"
        
        return f"SELECT {distinct} {select} FROM {from_} WHERE {where}"
    
    return query
'''
'''
def evaluate_query_plans(queries, stats, has_index):
    query_costs = {}
    for query in queries:
        cost = 0
        sql = sqlify(query)

        # Check if the query is a simple SELECT
        if "JOIN" not in sql:
            for column in stats:
                # Calculate cost based on number of distinct values
                cost += stats[column]["distinct_values"]
            query_costs[sql] = cost
        else:
            # Check if there's an index in either of the joined tables
            index_found = False
            for table in [table.strip() for table in sql.split("JOIN")[0:2]]:
                if has_index(table):
                    index_found = True
                    break
            # If there's an index, use index nested loop join
            if index_found:
                # Calculate cost based on size of smaller table
                join_tables = [table.strip() for table in sql.split("JOIN")]
                smaller_table = min([table for table in join_tables if table in stats], key=lambda x: stats[x]["size"])
                cost = stats[smaller_table]["size"]
                query_costs[sql] = cost
            else:
                # If no index, use block nested loop join
                join_tables = [table.strip() for table in sql.split("JOIN")]
                cost = sum([stats[table]["size"] for table in join_tables if table in stats])
                query_costs[sql] = cost
    return min(query_costs, key=query_costs.get)
    '''

def evaluate_query_plans(queries, stats, has_index):
    query_costs = {}
    for query in queries:
        cost = 0
        if "from" not in query:#we skip query plans that doesnt have from clause because they are not valid
            continue
        
        from_clause = query["from"]
        if isinstance(from_clause, str):
            # Check if the query is a simple SELECT from one table
            # Calculate cost based on number of distinct values-This approach is just a common heuristic for estimating query costs
            table_name = from_clause
            for column in stats[table_name]["columns"]:
                cost += stats[table_name]["columns"][column]["distinct_values"]
        elif isinstance(from_clause, dict):
            if "select" in from_clause:
                # Check if the query is a SELECT from a subquery
                subquery = from_clause
                subquery_cost = evaluate_query_plans([subquery], stats, has_index)
                cost = subquery_cost[subquery]
            elif "join" in from_clause:
                # Check if the query is a JOIN
                join_type = from_clause["join"]
                left_table = from_clause["left"]
                right_table = from_clause["right"]
                if has_index(left_table):
                    # If there's an index in the left table, use index nested loop join
                    cost = min(stats[left_table]["size"], stats[right_table]["size"])#This represents the number of times the inner table needs to be scanned
                else:
                    # If there's no index, use block nested loop join
                    cost = stats[left_table]["size"] * stats[right_table]["size"]
        query_costs[query] = cost
    
    print("Query costs:", query_costs)

    min_cost = float("inf")
    min_cost_query = None
    for query, cost in query_costs.items():
        if cost < min_cost:
            min_cost = cost
            min_cost_query = query
    
    return min_cost_query
'''
def evaluate_query_plans(queries, stats, has_index):
    query_costs = {}
    for query in queries:
        cost = 0
        if "from" not in query:
            continue
        
        from_clause = query["from"]
        if isinstance(from_clause, str):
            table_name = from_clause
            for column in stats[table_name]["columns"]:
                cost += stats[table_name]["columns"][column]["distinct_values"]
        elif isinstance(from_clause, dict):
            if "select" in from_clause:
                subquery = from_clause
                subquery_cost = evaluate_query_plans([subquery], stats, has_index)
                cost = subquery_cost[subquery]
            elif "join" in from_clause:
                join_type = from_clause["join"]
                left_table = from_clause["left"]
                right_table = from_clause["right"]
                if has_index(left_table):
                    cost = min(stats[left_table]["size"], stats[right_table]["size"])
                else:
                    cost = stats[left_table]["size"] * stats[right_table]["size"]
        if "where" in query:
            where_clause = query["where"]
            if isinstance(where_clause, dict) and "and" in where_clause:
                # Check if the WHERE clause is a conjunction of conditions
                left_condition = where_clause["and"]["left"]
                right_condition = where_clause["and"]["right"]
                if has_index(left_condition):
                    cost += 1
                elif has_index(right_condition):
                    cost += 1
                else:
                    cost += stats[table_name]["size"]
            elif isinstance(where_clause, str):
                if has_index(where_clause):
                    cost += 1
                else:
                    cost += stats[table_name]["size"]
        
        query_costs[query] = cost
    
    print("Query costs:", query_costs)

    min_cost = float("inf")
    min_cost_query = None
    for query, cost in query_costs.items():
        if cost < min_cost:
            min_cost = cost
            min_cost_query = query
    
    return min_cost_query

'''
'''
stats = {
    "table_1": {
        "size": 1000,
        "columns": {
            "col_1": {"distinct_values": 100},
            "col_2": {"distinct_values": 200},
            "col_3": {"distinct_values": 50},
        },
    },
    "table_2": {
        "size": 2000,
        "columns": {
            "col_4": {"distinct_values": 150},
            "col_5": {"distinct_values": 300},
        },
    },
}

'''