'''
https://en.wikipedia.org/wiki/Extendible_hashing
'''

class Node:
    '''
    Node abstraction. Represents a single bucket
    '''
    def __init__(self, b, values=None, ptrs=None):
        self.b = b # branching factor
        self.values = [] if values is None else values # Values (the data from the pk or uk column)
        self.ptrs = [] if ptrs is None else ptrs # ptrs (the indexes of each datapoint)
        self.__overflow = False


    def insert(self, value, ptr):
        '''
        Insert the value and its ptr to the node.

        Args:
            value: float. The value we are inserting to the node.
            ptr: float. The ptr of the inserted value (e.g. its index).
        '''
        self.values.append(value)
        self.ptrs.append(ptr)
        if len(self.values) < self.b:
            self.__overflow = False
        else:
            self.__overflow = True

    def delete_item(self, indx):
        self.values.pop(indx)
        self.ptrs.pop(indx)

    def is_overflow(self):
        return self.__overflow

    def show(self):
        '''
        Print the node's values and pointers.
        '''
        for i in range(len(self.values)):
            print(f"{self.ptrs[i]} --> {self.values[i]}" )


class HashTree():
    def __init__(self, b):
        '''
        The hash abstraction.
        '''
        self.b = b  # branching factor
        self.key_level = 1 # Number of digit used for the hash key
        self.nodes = []  # list of nodes. Every new node is appended here
        self.nodes.append(Node(self.b))
        self.nodes_idx = {'0': len(self.nodes)-1, '1': len(self.nodes)-1}  # hash index for every node

    def __hash__(self, data):
        hash_key = ''.join(format(ord(x), 'b') for x in str(data))
        if len(hash_key) < self.key_level:
            for i in range(len(hash_key),self.key_level+1):
                hash_key += '0'
        return hash_key

    def insert(self, value, ptr):
        '''
        Insert the value and its ptr to the appropriate node.

        Args:
            value: float. The input value.
            ptr: float. The ptr of the inserted value (e.g. its index).
        '''
        # find the index of the node that the value and its ptr should be inserted to (_search)
        node_hash_key = self.__hash__(value)
        index = self.nodes_idx[node_hash_key[:self.key_level]]
        if self.nodes[index].is_overflow():
            self.split(node_hash_key[:self.key_level])
            node_hash_key = self.__hash__(value)
            index = self.nodes_idx[node_hash_key[:self.key_level]]
        self.nodes[index].insert(value, ptr)

    def split(self, hash_key):
        '''
        Split the node with index=node_id.

        Args:
            hash_key: int. Hash key of node.
        '''
        # for key, old_node in self.nodes_idx.items():
        #     if old_node == node_id:
        old_node = self.nodes_idx[hash_key]
        self.nodes.append(Node(self.b))
        new_node = len(self.nodes) - 1
        if self.nodes_idx[hash_key[:-1]+'0'] == self.nodes_idx[hash_key[:-1]+'1']:
            self.nodes_idx[hash_key[:-1] + '0'] = old_node
            self.nodes_idx[hash_key[:-1] + '1'] = new_node
            for i in range(len(self.nodes[old_node].values)-1, 0, -1):
                # node_hash_key = ''.join(format(ord(x), 'b') for x in self.nodes[old_node].values[i])[:self.key_level]
                node_hash_key = self.__hash__(self.nodes[old_node].values[i])
                if node_hash_key[:self.key_level] == hash_key[:-1] + '1':
                    self.nodes[new_node].insert(self.nodes[old_node].values[i], self.nodes[old_node].ptrs[i])
                    self.nodes[old_node].delete_item(i)
        else:
            # increment number of digits used for the hash key
            self.key_level += 1

            new_nodes_idx={}
            for key, node_id in self.nodes_idx.items():
                if node_id == old_node:
                    new_nodes_idx[key + '0'] = old_node
                    new_nodes_idx[key + '1'] = new_node
                    for i in range(len(self.nodes[old_node].values) - 1, 0, -1):
                        # node_hash_key = ''.join(format(ord(x), 'b') for x in self.nodes[old_node].values[i])[:self.key_level]
                        node_hash_key = self.__hash__(self.nodes[old_node].values[i])
                        if node_hash_key[:self.key_level] == key + '1':
                            self.nodes[new_node].insert(self.nodes[old_node].values[i], self.nodes[old_node].ptrs[i])
                            self.nodes[old_node].delete_item(i)
                else:
                    new_nodes_idx[key + '0'] = node_id
                    new_nodes_idx[key + '1'] = node_id

            self.nodes_idx = new_nodes_idx

    def remove(self, value):
        # find the index of the node that the element should exist in
        node_hash_key = self.__hash__(value)
        index = self.nodes_idx[node_hash_key[:self.key_level]]
        item_idx = self.__search(index, value)
        if item_idx == -1:
            raise Exception(f'Key {value} is not exists.')
        else:
            self.nodes[index].delete_item(item_idx)

    def find(self, operator, value):
        '''
        Return ptr of element where hash_tree_value==value.
        Important, the user supplied "value" is the right value of the operation.
        The left value of the op is the hash tree value.

        Args:
            operator: string. The provided evaluation operator.
            value: float. The value being searched for.
        '''
        if operator == '=':
            # find the index of the node that the element should exist in
            node_hash_key = self.__hash__(value)
            index = self.nodes_idx[node_hash_key[:self.key_level]]
            item_idx=self.__search(index, value)
            if item_idx == -1:
                return []
            else:
                return [self.nodes[index].ptrs[item_idx]]
        else:
            raise Exception(f'Hash tree index supports only equality.')

    def __search(self, index, value):
        for i in range(len(self.nodes[index].values)):
            if self.nodes[index].values[i] == value:
                return i
        return -1

    def __str__(self):
        text = ''
        for key, index in self.nodes_idx.items():
            text += f"{key} ==> {index}\n"
            for i in range(len(self.nodes[index].values)):
                text += f"\t\t{self.nodes[index].ptrs[i]} --> {self.nodes[index].values[i]}\n"
        return text