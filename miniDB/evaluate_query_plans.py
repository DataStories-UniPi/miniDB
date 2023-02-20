import ast
import re


# A function that evaluates the cost of a subquery in the SELECT clause
def evaluate_select_clause(db, subquery):
    """

    Evaluates the SELECT clause for a given subquery and returns an estimate of the cost of the query.
    The cost is calculated as a combination of the number of rows in the table and the number of distinct values in each selected column,
    (the size of table is used as the cost when the sum of distinct values in each selected column is greater than the size of table)
    taking into account any filters in the WHERE clause. 
    This estimate is intended to provide a rough measure of the amount of data that needs to be read from disk and processed in memory in order to complete the query.
    Please note that this is just a heuristic for estimating query costs and may not be accurate in all cases.

    Args:
    db (database object): The database object containing the table data and statistics.
    subquery (dict): A dictionary representing the subquery to evaluate.

    Returns:
    int: The estimated cost of evaluating the SELECT clause.

    """
    # A dictionary that contains statistics about tables
    stats = db.stats
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
            # Check if there is an index on the left column
            if db._has_index(table_name,column_name_left):
                cost = 2
            # Check if there is an index on the right column
            elif db._has_index(table_name,column_name_right):
                cost = 2
            else:
            #Otherwise,add the table size to the cost
                cost += stats[table_name]["size"]
        elif isinstance(where_clause, str):
            # If there is only one condition in the "where" clause, check if the column has index
            column_name = re.findall(r'\w+', where_clause)[0]
            # If the column has an index, the cost is 2
            if db._has_index(table_name,column_name):
                cost = 2
            else:
                # Otherwise, add the size of the table to the cost
                cost += stats[table_name]["size"]
    #if cost is less than the table size add 1 to the cost.
    if cost < stats[table_name]["size"]:
        cost+=1
    else:
        # Add the size of the table to the cost  
        cost += stats[table_name]["size"]

    return cost,table_name


def evaluate_query_plans(db , queries):
    '''

    This function loops through each query and calculates its cost.
    If there is only one table in the FROM clause, the cost of each distinct value in each column is added,
    if the sum of of each distinct value in each column is less than the table size.
    If there are multiple tables in the FROM clause, the cost is calculated based on the size of the tables being joined.
    If one of the tables has an index on its primary key, its size is used as the cost(INLJ).
    Otherwise, the product of the sizes of the two tables is used(BNLJ).
    The cost of the query is also affected by any conditions specified in the WHERE clause.
    The query plan with the lowest cost is chosen as the optimal plan to execute.

    '''
    # Create a dictionary to store the costs of each query
    query_costs = {}
    # A dictionary that contains statistics about tables
    stats = db.stats
    # Loop through each query and calculate its cost
    for query in queries:
        cost = 0

        #cost_of_table symbolizes the cost of the table that we projected or selected
        cost_of_table = 0

        cost_of_distinct_values = 0
        #A bool that determines if INLJ is used
        INLJ = True
        #A bool that determines if BNLJ is used
        BNLJ = True
       
        # Check if the query has a "from" clause
        if "from" not in query:
            continue
        #if we have and in on_clause continue
        if "join" in query['from']:
            on_clasue = query['from']['on']
            if "and" in on_clasue:
                continue

         # Get the table(s) in the "from" clause
        from_clause = query["from"]
        # If there is only one table, add the cost of each distinct value in each column
        if isinstance(from_clause, str) :
            table_name = from_clause
            '''
            In general, the more distinct values there are in a table, the more processing may be required to perform operations that involve examining or manipulating the data.
            Therefore, estimating the cost of scanning a table based on the number of distinct values in each column can be a useful way to estimate the overall cost of queries that involve those columns.
            '''
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
                
                left_table = from_clause["left"]
                right_table = from_clause["right"]
                table_right = db.tables[right_table]

                #If the right table has an index on its primary key, use its size as the cost (INLJ)
                #On_clause supports only the join between the two tables on the primary key
                if db._has_index(right_table,table_right.pk):
                    cost += stats[right_table]["size"]
                    #add the size of the table to the cost_of_table
                    cost_of_table += stats[right_table]["size"]
                    table_name = right_table
                    #Set bool to False ,so we know we did INLJ
                    INLJ = False
                else:
                    # Otherwise, use the product of the sizes of the two tables as the cost (BNLJ)
                    cost += stats[left_table]["size"] * stats[right_table]["size"]
                    #add the size of the product of two table to the cost_of_table
                    cost_of_table += stats[left_table]["size"] * stats[right_table]["size"]
                    #Set bool to False ,so we know we did BNLJ
                    BNLJ = False

        # Check if the query has a "where" clause           
        if "where" in query and query['where'] != None:
            where_clause = query["where"]


            if isinstance(where_clause, dict) and "and" in where_clause:
                # If the "where" clause is a conjunction of conditions
                #Then check if the columns of conditions have an index
                left_condition = where_clause["and"]["left"]
                right_condition = where_clause["and"]["right"]
                column_name_left = re.findall(r'\w+', left_condition)[0]
                column_name_right = re.findall(r'\w+', right_condition)[0]

                if db._has_index(table_name,column_name_left):
                    #If the cost is zero add 1 to  the cost 
                    #If the left's condition column has an index, add 1 to the cost
                    if cost == 0:
                        cost += 1
                    cost += 1
                    
                elif db._has_index(table_name,column_name_right):
                    #If the cost is zero add 1 to  the cost 
                    #If the right's condition column has an index, add 1 to the cost
                    if cost == 0:
                        cost += 1
                    cost += 1
                else:
                    # Otherwise, add the size of the table to the cost
                    cost += stats[table_name]["size"]
                    #add the cost_of_table to the cost
                    cost += cost_of_table


            elif isinstance(where_clause, str):
                #if its a join, add the cost of the cost_of_table that we calculated earlier 
                if "join" in from_clause:
                    cost += cost_of_table
                # If there is only one condition in the "where" clause,then the column_name is the name of the column in our condition 
                column_name = re.findall(r'\w+', where_clause)[0]
                # if BNLJ True continue executing,otherwise add the cost_of_table to the cost
                if BNLJ:
                    #If the cost is 3 then we had an identity question with index in the subquery,so we add 1 to the cost 
                    if cost == 3:
                        cost += 1
                    #Check if table_name has an index on column_name 
                    elif db._has_index(table_name,column_name):
                        # If the condition's column has an index, add 1 to the cost
                        if cost == 0:
                            cost += 1 
                        cost += 1
                    #Otherwise,add the table_size + cost_of_table to the cost,but if INLJ happened then just add the cost_of_table to the cost
                    else:

                        if INLJ:
                            cost += stats[table_name]["size"]
                            cost += cost_of_table
                        else:
                            cost += cost_of_table
                else:
                    cost += cost_of_table
        else:
            cost += cost_of_table
        # Add the cost of the query to the dictionary of query costs
        query_costs[str(query)] = cost
    
        #print("Query cost:", query_costs.values())

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





