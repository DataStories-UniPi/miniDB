'''
https://en.wikipedia.org/wiki/B%2B_tree
'''

class Node:
    '''
    Node abstraction. Represents a single bucket
    '''
    def __init__(self, b, values=[], keys=[],\
                 left_sibling=None, right_sibling=None, parent=None, is_leaf=False):
        self.b = b # branching factor
        self.values = values # Values (the data from the pk column)
        self.keys = keys # Keys (the indexes of each datapoint or the index of another bucket)
        self.left_sibling = left_sibling # the index of a buckets left sibling
        self.right_sibling = right_sibling # the index of a buckets right sibling
        self.parent = parent # the index of a buckets parent
        self.is_leaf = is_leaf # a boolean value signaling whether the node is a leaf or not


    def find(self, value, return_ops=False):
        '''
        Returns the index of the next node to search for a value if the node is not a leaf (a keys of the available ones).
        If it is a leaf (we have found the appropriate node), it returns nothing.

        value: the value that we are searching for
        return_ops: set to True if you want to use the number of operations (for benchmarking)
        '''
        ops = 0 # number of operations (<>= etc). Used for benchmarking
        if self.is_leaf: #
            return

        # for each value in the node, if the user supplied value is smaller, return the btrees value index
        # else (no value in the node is larger) return the last key
        for index, existing_val in enumerate(self.values):
            ops+=1
            if value<existing_val:
                if return_ops:
                    return self.keys[index], ops
                else:
                    return self.keys[index]

        if return_ops:
            return self.keys[-1], ops
        else:
            return self.keys[-1]


    def insert(self, value, key, key1=None):
        '''
        Insert the value and its key/s to the appropriate place (node wise).
        User can input two keys to insert to a non leaf node.

        value: the value that we are inserting to the node
        key: the key of the inserted value (its index for example)
        key1: the 2nd key (in case the user wants to insert into a nonleaf node for ex)

        '''
        # for each value in the node, if the user supplied value is smaller, insert the value and its key into that position
        # if a second key is provided, insert it right next to the 1st key
        # else (no value in the node is larger) append value and key/s to the back of the list.
        for index, existing_val in enumerate(self.values):
            if value<existing_val:
                self.values.insert(index, value)
                self.keys.insert(index, key)
                if key1:
                    self.keys.insert(index+1, key1)
                return
        self.values.append(value)
        self.keys.append(key)
        if key1:
            self.keys.append(key1)

    def show(self):
        '''
        print the node's value and important info
        '''
        print('Values', self.values)
        print('Keys', self.keys)
        print('Parent', self.parent)
        print('LS', self.left_sibling)
        print('RS', self.right_sibling)


class Btree:
    def __init__(self, b):
        '''
        The tree abstraction.
        '''
        self.b = b # branching factor
        self.nodes = [] # list of nodes. Every new node is appended here
        self.root = None # the index of the root node

    def insert(self, value, key, rkey=None):
        '''
        Insert the value and its key/s to the appropriate node (node-level insertion is covered by the node object).
        User can input two keys to insert to a non leaf node.
        '''
        # if the tree is empty, add the first node and set the root index to 0 (the only node's index)
        if self.root is None:
            self.nodes.append(Node(self.b, is_leaf=True))
            self.root = 0

        # find the index of the node that the value and its key/s should be inserted to (_search)
        index = self._search(value)
        # insert to it
        self.nodes[index].insert(value,key)
        # if the node has more elements than b-1, split the node
        if len(self.nodes[index].values)==self.b:
            self.split(index)

    def _search(self, value, return_ops=False):
        '''
        Returns the index of the node that the given value exist or should exist in.

        value: the value that we are searching for
        return_ops: set to True if you want to use the number of operations (for benchmarking)
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
        Split the node with index=node_id
        '''
        # fetch the node to be split
        node = self.nodes[node_id
        # the value that will be propagated to the parent is the middle one.
        new_parent_value = node.values[len(node.values)//2]
        if node.is_leaf:
            # if the node is a leaf, the parent value should be a part of the new node (right)
            # Important: in a b+tree, every value should appear in a leaf
            right_values = node.values[len(node.values)//2:]
            right_keys   = node.keys[len(node.keys)//2:]

            # create the new node with the right half of the old nodes values and keys (including the middle ones)
            right = Node(self.b, right_values, right_keys,\
                         left_sibling=node_id, right_sibling=node.right_sibling, parent=node.parent, is_leaf=node.is_leaf)
            # since the new node (right) will be the next one to be appended to the nodes list
            # its index will be equal to the length of the nodes list.
            # Thus we set the old nodes (now left) right sibling to the right nodes future index (len of nodes)
            node.right_sibling = len(self.nodes)


        else:
            # if the node is not a leaf, the parent value shoudl NOT be part of the new node
            right_values = node.values[len(node.values)//2+1:]
            right_keys   = node.keys[len(node.keys)//2:]

            # if nonleafs should be connected change the following two lines and add siblings
            right = Node(self.b, right_values, right_keys,\
                        parent=node.parent, is_leaf=node.is_leaf)
            node.right_sibling = None
            for key in right_keys:
                self.nodes[key].parent = len(self.nodes)


        node.values = node.values[:len(node.values)//2]
        node.keys = node.keys[:len(node.keys)//2]

        self.nodes.append(right)

        if node.parent is None:
            # its the root that is split
            parent = Node(self.b, [new_parent_value], [node_id, len(self.nodes)-1]\
                          ,parent=node.parent, is_leaf=False)
            self.root = len(self.nodes)
            self.nodes.append(parent)
            node.parent = len(self.nodes)-1
            right.parent = len(self.nodes)-1
        else:
            self.nodes[node.parent].insert(new_parent_value, len(self.nodes)-1)
            if len(self.nodes[node.parent].values)==self.b:
                self.split(node.parent)



    def show(self):
        nds = []
        nds.append(self.root)
        for key in nds:
            if self.nodes[key].is_leaf:
                continue
            nds.extend(self.nodes[key].keys)

        for key in nds:
            print(f'## {key} ##')
            self.nodes[key].show()
            print('----')


    def find(self, value, operator='=='):
        results = []
        leaf_idx, ops = self._search(value, True)
        target_node = self.nodes[leaf_idx]

        if operator == '==':
            try:
                results.append(target_node.keys[target_node.values.index(value)])
                # print('Found')
            except:
                # print('Not found')
                pass

        if operator == '>':
            for idx, node_value in enumerate(target_node.values):
                ops+=1
                if value < node_value:
                    results.append(target_node.keys[idx])
            while target_node.right_sibling is not None:
                target_node = self.nodes[target_node.right_sibling]
                results.extend(target_node.keys)


        if operator == '>=':
            for idx, node_value in enumerate(target_node.values):
                ops+=1
                if value <= node_value:
                    results.append(target_node.keys[idx])
            while target_node.right_sibling is not None:
                target_node = self.nodes[target_node.right_sibling]
                results.extend(target_node.keys)

        if operator == '<':
            for idx, node_value in enumerate(target_node.values):
                ops+=1
                if value > node_value:
                    results.append(target_node.keys[idx])
            while target_node.left_sibling is not None:
                target_node = self.nodes[target_node.left_sibling]
                results.extend(target_node.keys)

        if operator == '<=':
            for idx, node_value in enumerate(target_node.values):
                ops+=1
                if value >= node_value:
                    results.append(target_node.keys[idx])
            while target_node.left_sibling is not None:
                target_node = self.nodes[target_node.left_sibling]
                results.extend(target_node.keys)

        print(f'With BTree -> {ops} eq operations')
        return results
