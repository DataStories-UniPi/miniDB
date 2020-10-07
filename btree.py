class Btree:
    def __init__(self, b=3):
        self.tree = {}
        self.h = 0
        self.b = b

    def add_level(self, level=-1):
        if level==-1:
            self.tree[self.h] = {}
            self.h += 1
#             print('New level')
        else:
            self.tree[level] = {}
            self.h += 1
#             print('New level')

    def add_node_to_level(self, level, index=-1, node=[[],[]]):
        if index==-1:
            self.tree[level][len(self.tree[level])] = node
        else:
            self.tree[level][index] = node


    def show(self):
        for i in range(len(self.tree)):
        #     print(i)
            for j in range(len(self.tree[i])):
                print(self.tree[i][j][0], end=' ')
                print(tuple(self.tree[i][j][1]), end='  ')
            print('\n')


    def search(self, value, level=0, index=0, pos=0):
#         print(f'Searching at {level, index, pos}')
        if self.h == 0:
            self.add_level()
            self.add_node_to_level(level)

        if level == self.h-1:
            return level, index


        if value < self.tree[level][index][0][pos]:
#             print('left')
            return self.search(value, level+1, self.tree[level][index][1][pos])
        elif pos < len(self.tree[level][index][0])-1:
#             print('next')
            return self.search(value, level, index, pos+1)
        else:
#             print('right')
            return self.search(value, level+1, self.tree[level][index][1][pos+1])


    def add_to_bucket(self, value, key, level, index, key2=None):
#         print(len(self.tree[level][index][0]))
        for ind, val in enumerate(self.tree[level][index][0]):
            if value<val:
                self.tree[level][index][0].insert(ind, value)
                self.tree[level][index][1].insert(ind, key)
                if key2 is not None:
                    self.tree[level][index][1].insert(ind+1, key2)

                return

        self.tree[level][index][0].append(value)
        self.tree[level][index][1].append(key)
        if key2 is not None:
            self.tree[level][index][1].append(key2)


    def split_node(self, level, index):
        if level == self.h-1:
            leaf = True
        else:
            leaf = False

#         print('leaf', leaf)

        new_pointer = self.tree[level][index][0][len(self.tree[level][index][0])//2]

        if leaf:
            left = [el[:len(el)//2] for el in self.tree[level][index]]
            right = [el[len(el)//2:] for el in self.tree[level][index]]
        else:
            left = [el[:len(el)//2] for el in self.tree[level][index]]
            right_values = self.tree[level][index][0][len(self.tree[level][index][0])//2+1:]
            right_keys   = self.tree[level][index][1][len(self.tree[level][index][1])//2:]
            right = [right_values, right_keys]
#         print(left, right)
        #make way if needed
        self.tree[level] = {(key+1 if key>index else key):val for key, val in self.tree[level].items()}
        self.add_node_to_level(level, index, left)
        self.add_node_to_level(level, index+1, right)

        self.add_to_parent(new_pointer, index, level)


    def add_to_parent(self, new_pointer, index, level):
        if level == 0:
            self.tree = {key+1:val for key, val in self.tree.items()}

            self.add_level(0)

            self.add_node_to_level(0,0,[[],[]])
            self.add_to_bucket(new_pointer, index, level, 0, index+1)
        else:
            parent = self.find_parent(index, level-1)
#             print(f'{new_pointer} parent is {parent, level-1}')
            self.add_to_bucket(new_pointer, index+1, level-1, parent)

#             print('parent', len(self.tree[level-1][parent]))
            if len(self.tree[level-1][parent][0])==self.b:
                self.split_node(level-1, parent)


    def find_parent(self, v, level):
        for key, value in self.tree[level].items():
            if v in value[1]:
                return key


    def insert(self, value, key):
        (level, index) = self.search(value)

        self.add_to_bucket(value, key, level, index)

        if len(self.tree[level][index][0])==self.b:
#             print('overflow')
            self.split_node(level, index)


    def find(self, element):
        level, index = self.search(element)
        if element not in self.tree[level][index][0]:
            print(f'{element} not found')

        else:
            ind = self.tree[level][index][0].index(element)
            return self.tree[level][index][1][ind]
