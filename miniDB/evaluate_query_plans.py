

# table_name = 'instructor'

# column_name = list(condition)[0]

# table = db.tables[table_name] # instance of Table class

# check if the table has an index and the column is the primary key

# if db._has_index(table_name) and column_name == table.column_names[table.pk_idx]:
import ast
import re


# A dictionary that contains statistics about tables
stats = {
    "instructor": {
        "size": 12,
        "columns": {
            "id": {"distinct_values": 12},
            "name": {"distinct_values": 12},
            "dept_name": {"distinct_values": 7},
            "salary": {"distinct_values": 11},
        },
    },
    "advisor": {
        "size": 9,
        "columns": {
            "s_id": {"distinct_values": 9},
            "i_id": {"distinct_values": 6},
        },
    },
    "student": {
        "size": 13,
        "columns": {
            "id": {"distinct_values": 13},
            "name": {"distinct_values": 13},
            "dept_name": {"distinct_values": 7},
            "tot_cred": {"distinct_values": 12},
        },
    },
}

# A function that evaluates the cost of a subquery in the SELECT clause
def evaluate_select_clause(db, subquery):
    """

    Evaluates the SELECT clause for a given table and list of columns, returning the resulting cost.

    The cost is calculated as the product of the number of rows in the table and the number of distinct values in each selected column.
    This is because for each selected column, a hash table must be constructed to keep track of the distinct values in that column,
    which requires memory proportional to the number of distinct values. The overall cost is thus a measure of the amount of data
    that needs to be read from disk and processed in memory in order to complete the query.

    Args:
        table_name (str): The name of the table to evaluate the SELECT clause for.
        select_list (list of str): A list of column names to select from the table.

    Returns:
        int: The cost of evaluating the SELECT clause.

    """
    cost = 0
    from_clause = subquery["from"]
    table_name = None
    if isinstance(from_clause, str):
        # Check if the query is a simple SELECT from one table
        table_name = from_clause
        # Calculate cost based on number of distinct values- This approach is just a common heuristic for estimating query costs
        cost_of_distinct_values = 0
        for column in stats[table_name]["columns"]:
            cost_of_distinct_values += stats[table_name]["columns"][column]["distinct_values"]
       
        if cost_of_distinct_values > stats[table_name]["size"]:
         # If the cost of scanning the entire table is lower than the cost of scanning each column for distinct values, add the size of the table to the cost
            cost += stats[table_name]["size"]

        else:
            #Otherwise, add the cost_of_distinct_values to the cost
            cost += cost_of_distinct_values
        
    elif isinstance(from_clause, dict):
        # Recursively evaluate the subquery
        if "select" in from_clause:
            subquery = from_clause
            cost,table_name = evaluate_select_clause(db, subquery)
            

    if "where" in subquery and subquery['where'] != None:
        where_clause = subquery["where"]
        if isinstance(where_clause, dict) and "and" in where_clause:
            # Check if the WHERE clause is a conjunction of conditions
            left_condition = where_clause["and"]["left"]
            right_condition = where_clause["and"]["right"]
            column_name_left = re.findall(r'\w+', left_condition)[0]
            column_name_right = re.findall(r'\w+', right_condition)[0]
            table = db.tables[table_name]
            # Check if there is an index on the primary key column
            if db._has_index(table_name) and column_name_left == table.column_names[table.pk_idx]:
                cost = 1
            elif db._has_index(table_name) and column_name_right == table.column_names[table.pk_idx]:
                cost = 1
            else:
                cost += stats[table_name]["size"]
        elif isinstance(where_clause, str):
             # If there is only one condition in the "where" clause, check if it uses the primary key
            column_name = re.findall(r'\w+', where_clause)[0]
            table = db.tables[table_name]
              # If the condition uses the primary key, the cost is 1
            if db._has_index(table_name) and column_name == table.column_names[table.pk_idx]:
                cost = 1
            else:
                # Otherwise, add the size of the table to the cost
                cost += stats[table_name]["size"]
    #if cost is less than the table size add the current cost to the cost.(This is the cost of select)
    if cost < stats[table_name]["size"]:
        cost+=cost
    else:
        # Add the size of the table to the cost  
        cost += stats[table_name]["size"]

             
    
    return cost,table_name


def evaluate_query_plans(db , queries):
    '''

    This function loops through each query and calculates its cost.
    If there is only one table in the FROM clause, the cost of each distinct value in each column is added.
    If there are multiple tables in the FROM clause, the cost is calculated based on the size of the tables being joined.
    If one of the tables has an index on its primary key, its size is used as the cost.
    Otherwise, the product of the sizes of the two tables is used.
    The cost of the query is also affected by any conditions specified in the WHERE clause.
    If the condition uses the primary key of the table, 1 is added to the cost.
    Otherwise, the size of the table is added to the cost. The query plan with the lowest cost is chosen as the optimal plan to execute.

    '''

    # Create a dictionary to store the costs of each query
    query_costs = {}

    # Loop through each query and calculate its cost
    for query in queries:
        cost = 0
        cost_of_table = 0
        cost_of_distinct_values = 0
       
        # Check if the query has a "from" clause
        if "from" not in query:
            continue
        #if we have and in on_clause continue
        on_clasue = query['from']['on']
        if "and" in on_clasue:
            continue

         # Get the table(s) in the "from" clause
        from_clause = query["from"]
        # If there is only one table, add the cost of each distinct value in each column
        if isinstance(from_clause, str) :
            table_name = from_clause
            
            for column in stats[table_name]["columns"]:
                cost_of_distinct_values += stats[table_name]["columns"][column]["distinct_values"]
            if cost_of_distinct_values > stats[table_name]["size"]:
                # If the cost of scanning the entire table is lower than the cost of scanning each column for distinct values, add the size of the table to the cost_of_table
                cost_of_table += stats[table_name]["size"]
            else:
                #Otherwise, add the cost_of_distinct_values to the cost_of_table
                cost_of_table += cost_of_distinct_values
        elif isinstance(from_clause, dict):
            # If there is a subquery in the "from" clause, evaluate the subquery to get its cost
            if "select" in from_clause:
                subquery = from_clause
                cost,table_name = evaluate_select_clause(db, subquery)
            elif "join" in from_clause:
                # If there is a join in the "from" clause, calculate the cost of the join
                
                join_type = from_clause["join"]
                left_table = from_clause["left"]
                right_table = from_clause["right"]
                

                # If the right table has an index on its primary key, use its size as the cost (INLJ)
                if db._has_index(right_table):
                    cost += stats[right_table]["size"]
                    cost_of_table += stats[right_table]["size"]
                    table_name = right_table
                else:
                    # Otherwise, use the product of the sizes of the two tables as the cost (BNLJ)
                    cost += stats[left_table]["size"] * stats[right_table]["size"]
                    cost_of_table += stats[left_table]["size"] * stats[right_table]["size"]
                    #We check if left table has an index or if its smaller than the right table ,if it has or if it smaller table_name = left_table, otherwise table_name = right_table
                    if db._has_index(left_table):
                        table_name = left_table
                    elif stats[left_table]["size"] < stats[right_table]["size"]:
                        table_name = left_table
                    else:
                        table_name = right_table


        # Check if the query has a "where" clause           
        if "where" in query and query['where'] != None:
            where_clause = query["where"]


            if isinstance(where_clause, dict) and "and" in where_clause:
                 # If the "where" clause is a conjunction of conditions, check if the conditions use the primary key

                left_condition = where_clause["and"]["left"]
                right_condition = where_clause["and"]["right"]
                column_name_left = re.findall(r'\w+', left_condition)[0]
                column_name_right = re.findall(r'\w+', right_condition)[0]
                table = db.tables[table_name]

                if db._has_index(table_name) and column_name_left == table.column_names[table.pk_idx]:
                     # If the left condition uses the primary key, add 1 to the cost
                    if cost == 0:
                        cost += 1
                    cost += 1
                    
                elif db._has_index(table_name) and column_name_right == table.column_names[table.pk_idx]:
                    # If the right condition uses the primary key, add 1 to the cost
                    if cost == 0:
                        cost += 1
                    cost += 1
                else:
                    # Otherwise, add the size of the table to the cost
                    cost += stats[table_name]["size"]
                    cost += cost_of_table


            elif isinstance(where_clause, str):
                #if its a join, add the cost of the cost_of_table that we calculated earlier 
                if "join" in from_clause:
                    cost += cost_of_table
                 # If there is only one condition in the "where" clause, check if it uses the primary key
                column_name = re.findall(r'\w+', where_clause)[0]
                table = db.tables[table_name]
                if db._has_index(table_name) and column_name == table.column_names[table.pk_idx]:
                     # If the condition uses the primary key, add 1 to the cost
                    if cost == 0:
                        cost += 1 
                    cost += 1
                else:
                    cost += stats[table_name]["size"]
                    cost += cost_of_table
        else:
            cost += cost_of_table
        # Add the cost of the query to the dictionary of query costs
        query_costs[str(query)] = cost
    

        print("Query cost:", query_costs.values())

    min_cost = float("inf")
    min_cost_query = None
    for query, cost in query_costs.items():
        if cost < min_cost:
            min_cost = cost
            min_cost_query = query
    min_cost_query = ast.literal_eval(min_cost_query)
    print(min_cost_query)
    print(min_cost)
    return min_cost_query





