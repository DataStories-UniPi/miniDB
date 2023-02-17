BYTES_IN_BLOCK = 256
import sys, math

class Node:
    def __init__(self):
        self.next_node = None
        self.parent_node = None
        self.operation = ''
        self.args = ''

def create_rel(dic):
    '''
    Creates a relational tree using a dictionary 
    '''

    start = Node()
    start.parent_node = None
    start.operation = 'Pi'
    start.args = dic['select']
    
    if dic['where'] != None:
        inter_node = Node()
        inter_node.parent_node = start
        inter_node.operation = 'Sigma'
        inter_node.args = dic['where']

    if isinstance(dic['from'], str):
        last_node = Node()
        last_node.operation = 'Get'
        last_node.args = dic['from']

    else:
        last_node = Node()
        last_node.operation = "Join"
        last_node.args = {"Join": dic['from']}

        kid1 = Node()
        kid1.operation = 'Get'
        kid1.args = dic['from']['left']

        kid2 = Node()
        kid2.operation = 'Get'
        kid2.args = dic['from']['right']

        kid1.parent_node = last_node
        kid2.parent_node = last_node

        last_node.next_node_left = kid1
        last_node.next_node_right = kid2

    if dic['where'] == None:
        start.next_node = last_node
        last_node.parent_node = start
    else:
        start.next_node = inter_node
        inter_node.parent_node = start
        inter_node.next_node = last_node
        last_node.parent_node = inter_node
    return start

def exec_rel(rel, db):
    '''
    Executes a relational tree 
    '''
    if rel == None:
        print("[~~~~~~~~~~~~~~~~~~~] Problem [~~~~~~~~~~~~~~~~~~~]")
        return
    if rel.operation == 'Get':
        val = db.select('*', rel.args, None)
        return val
    if rel.operation == 'Pi':
        val = exec_rel(rel.next_node, db)
        return db.select(rel.args, val, None)
    if rel.operation == 'Sigma':
        val = exec_rel(rel.next_node, db)
        return db.select('*', val, rel.args)
    if rel.operation == 'Join':
        val1 = exec_rel(rel.next_node_left, db)
        new_data1 = []
        for l in val1.data:
            new_data1.append(list(l))
        val1.data = new_data1
        val2 = exec_rel(rel.next_node_right, db)

        new_data2 = []
        for l in val2.data:
            new_data2.append(list(l))
        val2.data = new_data2

        return db.join(rel.args['Join']['join'], val1, val2, rel.args['Join']['on'])


def print_rel(rel):
    '''
    Prints a relational tree 
    '''

    print(rel.operation, rel.args)
    if rel.operation == 'Get':
        return
    if rel.operation == 'Pi':
        print_rel(rel.next_node)
    if rel.operation == 'Sigma':
        print_rel(rel.next_node)
    if rel.operation == 'Join':
        print('* ', end = '')
        print_rel(rel.next_node_left)
        print('* ', end = '')
        print_rel(rel.next_node_right)


def comp_cost(rel, db):
    '''
    Computes the total cost of a rel tree 
    '''

    if rel == None:
        print("[~~~~~~~~~~~~~~~~~~~] Problem [~~~~~~~~~~~~~~~~~~~]")
        return
    if rel.operation == 'Get':
        _, val = db.select('*', rel.args, None, return_cost=True)
        bytes_in_entry = sys.getsizeof(val.data[0])
        bfr = math.floor(BYTES_IN_BLOCK / bytes_in_entry)
        b = math.ceil(len(val.data) / bfr)
        # print("G>>> 0")
        return 0, val, b

    if rel.operation == 'Pi':
        curr_cost, val, b = comp_cost(rel.next_node, db)
        cost, n_val =  db.select(rel.args, val, None, return_cost=True)
        # print("P>>> ", b)
        return curr_cost  + b
    if rel.operation == 'Sigma':
        curr_cost, val, b = comp_cost(rel.next_node, db)

        cost, n_val = db.select('*', val, rel.args, return_cost=True)
        f_cost = 0

        bytes_in_entry = sys.getsizeof(val.data[0])
        bfr = math.floor(BYTES_IN_BLOCK / bytes_in_entry)
        b = math.ceil(len(n_val.data) / bfr)

        for cost_type in cost:
            if cost_type == 'linear':
                f_cost += b
            if cost_type.startswith('btree'):
                cost_type_n = cost_type.split(' ')
                if cost_type_n[1] == '=':
                    f_cost += int(cost_type_n[2])
                else:
                    f_cost += int(cost_type_n[2]) + b
            if cost_type.startswith('hash'):
                f_cost += 1
        # print("S>>> ", f_cost)
        return curr_cost + f_cost, n_val, b

    if rel.operation == 'Join':
        curr_cost1, val1, b1 = comp_cost(rel.next_node_left, db)
        curr_cost2, val2, b2 = comp_cost(rel.next_node_right, db)
        new_data1 = []

        for l in val1.data:
            new_data1.append(list(l))
        val1.data = new_data1

        new_data2 = []
        for l in val2.data:
            new_data2.append(list(l))
        val2.data = new_data2

        val =  db.join(rel.args['Join']['join'], val1, val2, rel.args['Join']['on'])
        bytes_in_entry = sys.getsizeof(val.data[0])
        bfr = math.floor(BYTES_IN_BLOCK / bytes_in_entry)
        b = math.ceil(len(val.data) / bfr)

        # print("J>>> ", 2 * b1 * math.log2(b1) + 2 * b2 *  math.log2(b2) + b1 + b2)
        return curr_cost1 + curr_cost2 + 2 * b1 * math.log2(b1) + 2 * b2 *  math.log2(b2) + b1 + b2, val, b


def create_alt_rels(rel):
    '''
    Creates the alternative rel trees from a starting rel tree (including the latter).
    '''

    all_rels = [rel]

    pivot = rel
    while pivot != None:
        if pivot.operation == 'Join':
            break

        pivot = pivot.next_node
    
    if pivot != None and pivot.parent_node.operation == 'Sigma':
        condition = pivot.parent_node.args
        tables_in_condition = []
        for or_island in condition:
            for stmt in or_island:
                tables_in_condition.append(stmt.split(".")[0])

        all_conditions_left = True
        all_conditions_right = True
        for table in tables_in_condition:
            if table != pivot.args['Join']['left']:
                all_conditions_left = False
            if table != pivot.args['Join']['right']:
                all_conditions_right = False

        if all_conditions_left:
            pivot1 = rel
            n = Node()
            all_rels.append(n)
            while pivot1 != pivot.parent_node:
                n.operation = pivot1.operation
                n.args = pivot1.args
                k = Node()
                n.next_node = k
                n = k

                pivot1 = pivot1.next_node
            
            k.operation = pivot.operation
            k.args = pivot.args

            sigma = Node()
            sigma.operation = pivot.parent_node.operation
            new_args = []
            for or_island in pivot.parent_node.args:
                new_isl = []
                for stmt in or_island:
                    new_isl.append(stmt.replace(pivot.args['Join']['left'] + ".", ''))
                new_args.append(new_isl)
            
            sigma.args = new_args

            k.next_node_left = sigma
            
            sigma.next_node = pivot.next_node_left
            k.next_node_right = pivot.next_node_right



        if all_conditions_right:
            pivot1 = rel
            n = Node()
            all_rels.append(n)
            while pivot1 != pivot.parent_node:
                n.operation = pivot1.operation
                n.args = pivot1.args
                k = Node()
                n.next_node = k
                n = k

                pivot1 = pivot1.next_node
            
            k.operation = pivot.operation
            k.args = pivot.args

            sigma = Node()
            sigma.operation = pivot.parent_node.operation
            new_args = []
            for or_island in pivot.parent_node.args:
                new_isl = []
                for stmt in or_island:
                    new_isl.append(stmt.replace(pivot.args['Join']['right'] + ".", ''))
                new_args.append(new_isl)
            
            sigma.args = new_args

            k.next_node_right = sigma
            
            sigma.next_node = pivot.next_node_right
            k.next_node_left = pivot.next_node_left

        if not all_conditions_right and not all_conditions_left and len(pivot.parent_node.args) == 1:
            pivot1 = rel
            n = Node()
            all_rels.append(n)
            while pivot1 != pivot.parent_node:
                n.operation = pivot1.operation
                n.args = pivot1.args
                k = Node()
                n.next_node = k
                n = k

                pivot1 = pivot1.next_node
            
            k.operation = pivot.operation
            k.args = pivot.args

            new_args1 = []
            new_args2 = []

            for stmt in pivot.parent_node.args[0]:
                if stmt.count(pivot.args['Join']['left'] + ".") == 1:
                    new_args1.append(stmt.replace(pivot.args['Join']['left'] + ".", ''))
                else:
                    new_args2.append(stmt.replace(pivot.args['Join']['right'] + ".", ''))
        
            sigma1 = Node()
            sigma2 = Node()
            sigma1.operation = pivot.parent_node.operation
            sigma2.operation = pivot.parent_node.operation

            sigma1.args = [new_args1]
            sigma2.args = [new_args2]

            sigma1.next_node = pivot.next_node_left
            sigma2.next_node = pivot.next_node_right

            k.next_node_left = sigma1
            k.next_node_right = sigma2
            
    return all_rels
