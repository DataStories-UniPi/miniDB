
'''The following code defines a hash table data structure 
that uses the LSB variant hash function to generate a hash value for a given key. 
The hash table is implemented using a list of buckets,
where each bucket contains a list of key-value pairs.'''

class Hash:
    def __init__(self, size):
        self.size = size
        self.table = [None] * size

    def hash_function(self, value):
        """Calculate the hash value for a given value."""
        # convert the value to an integer using the built-in hash function
        value_hash = hash(value)
        # apply the LSB variant hash function using bitwise AND
        hash_val = value_hash & (2**self.size - 1)
        # handle the case where the hash value is larger than the size of the hash table
        if hash_val >= self.size:
            hash_val = hash_val % self.size
        return hash_val

    def insert(self, key, value):
        index = self.hash_function(key)
        if self.table[index] is None:
            self.table[index] = Bucket()
        self.table[index].insert(key, value)

    def delete(self, key):
        index = self.hash_function(key)
        if self.table[index] is None:
            return False
        else:
            return self.table[index].delete(key)

    def select(self, key):
        index = self.hash_function(key)
        if self.table[index] is None:
            return None
        else:
            return self.table[index].select(key)

    def show(self):
        for i in range(self.size):
            bucket = self.table[i]
            if bucket is None:
                print(f'[{i}]')
            else:
                print(f'[{i}]', end=' ')
                for k, v in bucket.items:
                    print(f'({k}: {v})', end=' ')
                print()

    def get(self, key):
        index = self.hash_function(key)
        return self.table[index].get(key)


class Bucket:
    def __init__(self):
        self.items = []

    def insert(self, key, value):
        self.items.append((key, value))

    def delete(self, key):
        for i, (k, v) in enumerate(self.items):
            if k == key:
                del self.items[i]
                return True
        return False

    def select(self, key):
        for k, v in self.items:
            if k == key:
                return v
        return None

    def get(self, index):
        if index < len(self.items):
            return self.items[index][1]
        else:
            return None
