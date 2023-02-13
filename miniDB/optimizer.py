class Node:
    def __init__(self):
        self.next_node = None
        self.parent_node = None
        self.operation = ''
        self.args = ''


def create_rel(dic):
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
        start.exec_start = [last_node]

    else:
        last_node = Node()
        last_node.operation = "Join"
        last_node.args = {"Join": dic['from']['join']}

        kid1 = Node()
        kid1.operation = 'Get'
        kid1.side = 'left'

        kid1.args = dic['from']['left']

        kid2 = Node()
        kid2.operation = 'Get'
        kid2.side = 'right'

        kid2.args = dic['from']['right']

        kid1.parent_node = last_node
        kid2.parent_node = last_node

        last_node.next_node_left = kid1
        last_node.next_node_right = kid2
        start.exec_start = [kid1, kid2]

    if dic['where'] == None:
        start.next_node = last_node
        last_node.parent_node = start
    else:
        start.next_node = inter_node
        inter_node.parent_node = start
        inter_node.next_node = last_node
        last_node.parent_node = inter_node
    return start


# def _exec_rel(rel):
#     pivot = rel
#     while pivot != None:
#         print(pivot.operation, pivot.args)
#         if pivot.parent_node != None and pivot.parent_node.operation == 'Join' and pivot.side == 'left':
#             return
#         pivot = pivot.parent_node

# def exec_rel(rel):
#     for r in rel.exec_start:
#         _exec_rel(r)


def print_rel(rel):
    pivot = rel
    while pivot != None:
        print(pivot.operation, pivot.args)
        if pivot.operation == 'Join':
            print_rel(pivot.next_node_left)
            print_rel(pivot.next_node_right)
        pivot = pivot.next_node

# def create_alt_rels(rel):
#     all_rels = [rel]

#     pivot = rel
#     while pivot != None:
#         if pivot.operation == 'Pi' and pivot.next_node.operation == 'Sigma':

#             if pivot == rel:
#                 start = Node()
#                 start.operation = pivot.next_node.operation
#                 start.args = pivot.next_node.args

#                 second = Node()
#                 second.operation = pivot.operation
#                 second.args = pivot.args

#                 start.next_node = second
#                 t_pivot = second
#                 pivot1 = pivot.next_node.next_node
#                 while pivot1 != None:
#                     t1 = Node()
#                     t1.operation = pivot1.operation
#                     t1.args = pivot1.args

#                     t_pivot.next_node = t1
#                     t_pivot = t1
#                     pivot1 = pivot1.next_node

#                 all_rels.append(start)
#             else:
#                 pivot1 = Node()
#                 pivot1.operation =rel.operation
#                 pivot1.args = rel.args
#                 t_pivot = pivot1

#                 act_pivot = rel.next_node
#                 while act_pivot != pivot:
#                     t1 = Node()
#                     t1.operation = act_pivot.operation
#                     t1.args = act_pivot.args

#                     t_pivot.next_node = t1
#                     t_pivot = t1
#                     act_pivot = act_pivot.next_node


#                 next_n = Node()
#                 next_n.operation = act_pivot.next_node.operation
#                 next_n.args = act_pivot.next_node.args

#                 next_next_n = Node()
#                 next_next_n.operation = act_pivot.operation
#                 next_next_n.args = act_pivot.args

#                 t_pivot.next_node = next_n
#                 next_n.next_node = next_next_n
#                 t_pivot = next_next_n

#                 act_pivot  = act_pivot.next_node.next_node.next_node

#                 while act_pivot != pivot:
#                     t1 = Node()
#                     t1.operation = act_pivot.operation
#                     t1.args = act_pivot.args

#                     t_pivot.next_node = t1
#                     t_pivot = t1
#                     act_pivot = act_pivot.next_node

#         pivot = pivot.next_node
#     return all_rels


# def create_alt_rels(rel):
#     all_rels = [rel]

#     pivot = rel
#     while pivot != None:
#         if pivot.operation == 'Pi' and pivot.next_node.operation == 'Sigma':

#             if pivot == rel:
#                 start = Node()
#                 start.operation = pivot.next_node.operation
#                 start.args = pivot.next_node.args

#                 second = Node()
#                 second.operation = pivot.operation
#                 second.args = pivot.args

#                 start.next_node = second
#                 t_pivot = second
#                 pivot1 = pivot.next_node.next_node
#                 while pivot1 != None:
#                     t1 = Node()
#                     t1.operation = pivot1.operation
#                     t1.args = pivot1.args

#                     t_pivot.next_node = t1
#                     t_pivot = t1
#                     pivot1 = pivot1.next_node

#                 all_rels.append(start)
#             else:
#                 pivot1 = Node()
#                 pivot1.operation =rel.operation
#                 pivot1.args = rel.args
#                 t_pivot = pivot1

#                 act_pivot = rel.next_node
#                 while act_pivot != pivot:
#                     t1 = Node()
#                     t1.operation = act_pivot.operation
#                     t1.args = act_pivot.args

#                     t_pivot.next_node = t1
#                     t_pivot = t1
#                     act_pivot = act_pivot.next_node


#                 next_n = Node()
#                 next_n.operation = act_pivot.next_node.operation
#                 next_n.args = act_pivot.next_node.args

#                 next_next_n = Node()
#                 next_next_n.operation = act_pivot.operation
#                 next_next_n.args = act_pivot.args

#                 t_pivot.next_node = next_n
#                 next_n.next_node = next_next_n
#                 t_pivot = next_next_n

#                 act_pivot  = act_pivot.next_node.next_node.next_node

#                 while act_pivot != pivot:
#                     t1 = Node()
#                     t1.operation = act_pivot.operation
#                     t1.args = act_pivot.args

#                     t_pivot.next_node = t1
#                     t_pivot = t1
#                     act_pivot = act_pivot.next_node

#         pivot = pivot.next_node
#     return all_rels
