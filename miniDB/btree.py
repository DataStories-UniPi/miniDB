'''
https://en.wikipedia.org/wiki/B%2B_tree
'''

class Node:
    '''
    Node abstraction. Represents a single bucket
    '''
    def __init__(self, b, values=[], ptrs=[],\
                 left_sibling=None, right_sibling=None, parent=None, is_leaf=False):
        self.b = b # branching factor
        self.values = values # Values (the data from the selected columns)
        self.ptrs = ptrs # ptrs (the indexes of each datapoint or the index of another bucket)
        self.left_sibling = left_sibling # the index of a buckets left sibling
        self.right_sibling = right_sibling # the index of a buckets right sibling
        self.parent = parent # the index of a buckets parent
        self.is_leaf = is_leaf # a boolean value signaling whether the node is a leaf or not


    def find(self, value, return_ops=False):
        '''
        Returns the index of the next node to search for a value if the node is not a leaf (a ptrs of the available ones).
        If it is a leaf (we have found the appropriate node), nothing is returned.

        Args:
            value: string. The value being searched for.
            return_ops: boolean. Set to True if you want to use the number of operations (for benchmarking).
        '''
        ops = 0 # number of operations (<>= etc). Used for benchmarking
        if self.is_leaf: #
            return


        # for each value in the node, if the user supplied value is smaller, return the btrees value index
        # else (no value in the node is larger) return the last ptr
        for index, existing_val in enumerate(self.values):
            ops+=1
            if value<existing_val:
                if return_ops:
                    return self.ptrs[index], ops
                else:
                    return self.ptrs[index]

        if return_ops:
            return self.ptrs[-1], ops
        else:
            return self.ptrs[-1]


    def insert(self, value, ptr, ptr1=None):
        '''
        Insert the value and its ptr/s to the appropriate place (node wise).
        User can input two ptrs to insert to a non leaf node.

        Args:
            value: string. The value we are inserting to the node.
            ptr: float. The ptr of the inserted value (e.g. its index).
            ptr1: float. The 2nd ptr (e.g. in case the user wants to insert into a nonleaf node).
        '''
        # for each value in the node, if the user supplied value is smaller, insert the value and its ptr into that position
        # if a second ptr is provided, insert it right next to the 1st ptr
        # else (no value in the node is larger) append value and ptr/s to the back of the list.

        for index, existing_val in enumerate(self.values):
            if value<existing_val:

                self.values.insert(index, value)
                self.ptrs.insert(index+1, ptr)

                if ptr1:
                    self.ptrs.insert(index+1, ptr1)
                return
        self.values.append(value)
        self.ptrs.append(ptr)
        if ptr1:
            self.ptrs.append(ptr1)
        print(self.values)



    def show(self):
        '''
        Print the node's value and relevant information.
        '''
        print('Values', self.values)
        print('ptrs', self.ptrs)
        print('Parent', self.parent)
        print('LS', self.left_sibling)
        print('RS', self.right_sibling)


class Btree:
    def __init__(self, index_name, b, is_duplicate= False):
        '''
        The tree abstraction.
        '''
        self.index_name = index_name
        self.b = b # branching factor
        self.is_duplicate = is_duplicate # true if we are indexing a column with duplicates
        self.nodes = [] # list of nodes. Every new node is appended here
        self.root = None # the index of the root node

    def insert(self, value, ptr, rptr=None):
        '''
        Insert the value and its ptr/s to the appropriate node (node-level insertion is covered by the node object).
        User can input two ptrs to insert to a non leaf node.

        Args:
            value: string. The input value.
            ptr: float. The ptr of the inserted value to the table (e.g. its index).
        '''
        # if the tree is empty, add the first node and set the root index to 0 (the only node's index)
        if self.root is None:
            self.nodes.append(Node(self.b, is_leaf=True))
            self.root = 0

        # find the index of the node that the value and its ptr/s should be inserted to (_search)
        index = self._search(value)
        # insert to it
        self.nodes[index].insert(value,ptr)
        # if the node has more elements than b-1, split the node
        if len(self.nodes[index].values)==self.b:
            self.split(index)

    def _search(self, value, return_ops=False):
        '''
        Returns the index of the node that the given value exists or should exist in.

        Args:
            value: string. The value being searched for.
            return_ops: boolean. Set to True if you want to use the number of operations (for benchmarking).
        '''
        ops=0 # number of operations (<>= etc). Used for benchmarking

        #start with the root node
        node = self.nodes[self.root]
        # while the node that we are searching in is not a leaf
        # keep searching
        while not node.is_leaf:
            idx, ops1 = node.find(value, return_ops=True)
            node = self.nodes[idx]
            ops += ops1

        # finally return the index of the appropriate node (and the ops if you want to)
        if return_ops:
            return self.nodes.index(node), ops
        else:
            return self.nodes.index(node)


    def split(self, node_id):
        '''
        Split the node with index=node_id.

        Args:
            node_id: float. The corresponding ID of the node.
        '''
        # fetch the node to be split
        node = self.nodes[node_id]
        # the value that will be propagated to the parent is the middle one.
        new_parent_value = node.values[len(node.values)//2]
        if node.is_leaf:
            # if the node is a leaf, the parent value should be a part of the new node (right)
            # Important: in a b+tree, every value should appear in a leaf
            right_values = node.values[len(node.values)//2:]
            right_ptrs   = node.ptrs[len(node.ptrs)//2:]

            # create the new node with the right half of the old nodes values and ptrs (including the middle ones)
            right = Node(self.b, right_values, right_ptrs,\
                         left_sibling=node_id, right_sibling=node.right_sibling, parent=node.parent, is_leaf=node.is_leaf)
            # since the new node (right) will be the next one to be appended to the nodes list
            # its index will be equal to the length of the nodes list.
            # Thus we set the old nodes (now left) right sibling to the right nodes future index (len of nodes)
            if node.right_sibling is not None:
                self.nodes[node.right_sibling].left_sibling = len(self.nodes)
            node.right_sibling = len(self.nodes)


        else:
            # if the node is not a leaf, the parent value shoudl NOT be part of the new node
            right_values = node.values[len(node.values)//2+1:]
            if self.b%2==1:
                right_ptrs = node.ptrs[len(node.ptrs)//2:]
            else:
                right_ptrs = node.ptrs[len(node.ptrs)//2+1:]

            # if nonleafs should be connected change the following two lines and add siblings
            right = Node(self.b, right_values, right_ptrs,\
                        parent=node.parent, is_leaf=node.is_leaf)
            # make sure that a non leaf node doesnt have a parent
            node.right_sibling = None
            # the right node's kids should have him as a parent (if not all nodes will have left as parent)
            for ptr in right_ptrs:
                self.nodes[ptr].parent = len(self.nodes)

        # old node (left) keeps only the first half of the values/ptrs
        node.values = node.values[:len(node.values)//2]
        if self.b%2==1:
            node.ptrs = node.ptrs[:len(node.ptrs)//2]
        else:
            node.ptrs = node.ptrs[:len(node.ptrs)//2+1]

        # append the new node (right) to the nodes list
        self.nodes.append(right)

        # If the new nodes have no parents (a new level needs to be added
        if node.parent is None:
            # its the root that is split
            # new root contains the parent value and ptrs to the two recently split nodes
            parent = Node(self.b, [new_parent_value], [node_id, len(self.nodes)-1]\
                          ,parent=node.parent, is_leaf=False)

            # set root, and parent of split celss to the index of the new root node (len of nodes-1)
            self.nodes.append(parent)
            self.root = len(self.nodes)-1
            node.parent = len(self.nodes)-1
            right.parent = len(self.nodes)-1
        else:
            # insert the parent value to the parent

            self.nodes[node.parent].insert(new_parent_value, len(self.nodes)-1)
            # check whether the parent needs to be split
            if len(self.nodes[node.parent].values)==self.b:
                self.split(node.parent)




    def show(self):
        '''
        Show important info for each node (sort by level - root first, then left to right).
        '''
        nds = []
        nds.append(self.root)
        for ptr in nds:
            if self.nodes[ptr].is_leaf:
                continue
            nds.extend(self.nodes[ptr].ptrs)

        for ptr in nds:
            print(f'## {ptr} ##')
            self.nodes[ptr].show()
            print('----')


    def plot(self):
        ## arrange the nodes top to bottom left to right
        nds = []
        nds.append(self.root)
        for ptr in nds:
            if self.nodes[ptr].is_leaf:
                continue
            nds.extend(self.nodes[ptr].ptrs)

        # add each node and each link
        g = 'digraph G{\nforcelabels=true;\n'

        for i in nds:
            node = self.nodes[i]
            g+=f'{i} [label="{node.values}"]\n'
            if node.is_leaf:
                continue
                # if node.left_sibling is not None:
                #     g+=f'"{node.values}"->"{self.nodes[node.left_sibling].values}" [color="blue" constraint=false];\n'
                # if node.right_sibling is not None:
                #     g+=f'"{node.values}"->"{self.nodes[node.right_sibling].values}" [color="green" constraint=false];\n'
                #
                # g+=f'"{node.values}"->"{self.nodes[node.parent].values}" [color="red" constraint=false];\n'
            else:
                for child in node.ptrs:
                    g+=f'{child} [label="{self.nodes[child].values}"]\n'
                    g+=f'{i}->{child};\n'
        g +="}"

        try:
            from graphviz import Source
            src = Source(g)
            src.render('bplustree', view=True)
        except ImportError:
            print('"graphviz" package not found. Writing to graph.gv.')
            with open('graph.gv','w') as f:
                f.write(g)

    def find(self, operator, value):
        '''
        Return ptrs of elements where btree_value"operator"value.
        Important, the user supplied "value" is the right value of the operation. That is why the operation are reversed below.
        The left value of the op is the btree value.

        Args:
            operator: list of strings. The provided evaluation operator for every column.
            value: string. The value being searched for.
        '''
        results = []
        print(value)
        operator = tuple(operator)
        # find the index of the node that the element should exist in
        # case 1: element is primary key
        if not self.is_duplicate:
            value = tuple(value)
            leaf_idx, ops = self._search(value, True)
            target_node = self.nodes[leaf_idx]
            if operator[len(operator)-1] == '=':
                # if the element exist, append to list, else pass and return
                try:
                    results.append(target_node.ptrs[target_node.values.index(value)])
                    # print('Found')
                except:
                    # print('Not found')
                    pass

            # for all other ops, the code is the same, only the operations themselves and the sibling indexes change
            # for > and >= (btree value is >/>= of user supplied value), we return all the right siblings (all values are larger than current cell)
            # for < and <= (btree value is </<= of user supplied value), we return all the left siblings (all values are smaller than current cell)

            elif operator[len(operator)-1] == '>':
                for idx, node_value in enumerate(target_node.values):
                    ops+=1
                    if node_value > value:
                        results.append(target_node.ptrs[idx])
                while target_node.right_sibling is not None:
                    target_node = self.nodes[target_node.right_sibling]
                    for data in target_node:
                        if data[0] != value[0]:
                            return results
                        else:
                            results.append(target_node.ptrs[target_node.values.index(data)])


            elif operator[len(operator)-1] == '>=':
                for idx, node_value in enumerate(target_node.values):
                    ops+=1
                    if node_value >= value:
                        results.append(target_node.ptrs[idx])
                while target_node.right_sibling is not None:
                    target_node = self.nodes[target_node.right_sibling]
                    for data in target_node:
                        if data[0] != value[0]:
                            return results
                        else:
                            results.append(target_node.ptrs[target_node.values.index(data)])

            elif operator[len(operator)-1] == '<':
                for idx, node_value in enumerate(target_node.values):
                    ops+=1
                    if node_value < value:
                        results.append(target_node.ptrs[idx])
                while target_node.left_sibling is not None:
                    target_node = self.nodes[target_node.left_sibling]
                    for data in target_node:
                        if data[0] != value[0]:
                            return results
                        else:
                            results.append(target_node.ptrs[target_node.values.index(data)])

            elif operator[len(operator)-1] == '<=':
                for idx, node_value in enumerate(target_node.values):
                    ops+=1
                    if node_value <= value:
                        results.append(target_node.ptrs[idx])
                while target_node.left_sibling is not None:
                    target_node = self.nodes[target_node.left_sibling]
                    for data in target_node:
                        if data[0] != value[0]:
                            return results
                        else:
                            results.append(target_node.ptrs[target_node.values.index(data)])

            else:
                print('Something really wrong happened')
            # print the number of operations (usefull for benchamrking)
            # print(f'With BTree -> {ops} comparison operations')
            return results
        # case 2: if we have duplicates - everything other than the pk for now
        else:
            if operator[len(operator)-1] == '=':
                print(type(value))
                value = value.append(0)
                print(type(value))
                print(value)
                value = tuple(value)
                leaf_idx, ops = self._search(value, True)
                target_node = self.nodes[leaf_idx]
                print(self.nodes[leaf_idx].values)
                for idx, node_value in enumerate(target_node.values):
                    ops += 1
                    #print('node value')
                    #print(node_value[0].replace(" ", ""))
                    #print('value')
                    #print(value[0])
                    # if we find a match
                    if node_value[0].replace(" ", "") == value[0]:
                        print('hello ioakeim')
                        print(node_value[0].replace(" ", ""))
                        print(node_value[1].replace(" ", ""))
                        results.append(node_value[1])
                while target_node.right_sibling is not None:
                    target_node = self.nodes[target_node.right_sibling]
                    print(target_node)
                    for data in enumerate(target_node.values):
                        if data[1][0] != value[0]:  
                            print('evi1')
                            print(data[1][0])
                            return results
                        else:
                            print('evi2')
                            print(data[1][0])
                            print(data[1][1])
                            results.append(data[1][1])
                return results

            if operator[len(operator)-1] == '>=':
                value = tuple(value.append(0))
                print(value)
                leaf_idx, ops = self._search(tuple(value), True)
                target_node = self.nodes[leaf_idx]
                print(self.nodes[leaf_idx].values)
                for idx, node_value in enumerate(target_node.values):
                    ops += 1
                    if node_value >= value:
                        results.append(target_node.ptrs[idx])
                while target_node.right_sibling is not None:
                    target_node = self.nodes[target_node.right_sibling]
                    results.extend(target_node.ptrs)

            if operator[len(operator)-1] == '<':
                value = tuple(value.append(0))
                print(value)
                leaf_idx, ops = self._search(tuple(value), True)
                target_node = self.nodes[leaf_idx]
                for idx, node_value in enumerate(target_node.values):
                    ops += 1
                    if node_value < value:
                        results.append(target_node.ptrs[idx])
                while target_node.left_sibling is not None:
                    target_node = self.nodes[target_node.left_sibling]
                    results.extend(target_node.ptrs)

            if operator[len(operator)-1] == '<=':
                value = tuple(value.append(2000000000000))
                print(value)
                leaf_idx, ops = self._search(tuple(value), True)
                target_node = self.nodes[leaf_idx]
                for idx, node_value in enumerate(target_node.values):
                    ops += 1
                    if node_value <= value:
                        results.append(target_node.ptrs[idx])
                while target_node.left_sibling is not None:
                    target_node = self.nodes[target_node.left_sibling]
                    results.extend(target_node.ptrs)

            if operator[len(operator)-1] == '>':
                value = tuple(value.append(2000000000000))
                print(value)
                leaf_idx, ops = self._search(tuple(value), True)
                target_node = self.nodes[leaf_idx]
                for idx, node_value in enumerate(target_node.values):
                    ops += 1
                    if node_value > value:
                        results.append(target_node.ptrs[idx])
                while target_node.right_sibling is not None:
                    target_node = self.nodes[target_node.right_sibling]
                    results.extend(target_node.ptrs)

    #TODO: delete index
    #def __del__(self):

