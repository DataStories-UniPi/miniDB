class Node:
    def __init__(self, b, values=[], keys=[],\
                 left_sibling=None, right_sibling=None, parent=None, is_leaf=False):
        self.b = b
        self.values = values
        self.keys = keys
        self.left_sibling = left_sibling
        self.right_sibling = right_sibling
        self.parent = parent
        self.is_leaf = is_leaf


    def find(self, value, return_ops=False):
        ops = 0
        if self.is_leaf:
            return
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
        for index, existing_val in enumerate(self.values):
            if value<existing_val:
                self.values.insert(index, value)
                self.keys.insert(index, key)
                return
        self.values.append(value)
        self.keys.append(key)
        if key1:
            self.keys.append(key1)

    def show(self):
        print('Values', self.values)
        print('Keys', self.keys)
        print('Parent', self.parent)
        print('LS', self.left_sibling)
        print('RS', self.right_sibling)


class Btree:
    def __init__(self, b):
        self.b = b
        self.nodes = []
        self.root = None

    def insert(self, value, key, rkey=None):
        if self.root is None:
            self.nodes.append(Node(self.b, is_leaf=True))
            self.root = 0
        index = self._search(value)
        self.nodes[index].insert(value,key)
        if len(self.nodes[index].values)==self.b:
            self.split(index)

    def _search(self, value, return_ops=False):
        ops=0
        node = self.nodes[self.root]
        while not node.is_leaf:
            idx, ops1 = node.find(value, return_ops=True)
            node = self.nodes[idx]
            ops += ops1

        if return_ops:
            return self.nodes.index(node), ops
        else:
            return self.nodes.index(node)


    def split(self, node_id):
        node = self.nodes[node_id]
        new_parent_value = node.values[len(node.values)//2]
        if node.is_leaf:
            right_values = node.values[len(node.values)//2:]
            right_keys   = node.keys[len(node.keys)//2:]

            right = Node(self.b, right_values, right_keys,\
                         left_sibling=node_id, right_sibling=node.right_sibling, parent=node.parent, is_leaf=node.is_leaf)
            node.right_sibling = len(self.nodes)


        else:
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


    # def show(self):
    #     for pos, node in enumerate(self.nodes):
    #         print(f'## {pos} ##')
    #         node.show()
    #         print('----')

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
