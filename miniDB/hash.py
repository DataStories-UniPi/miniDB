class Hash:
    def __init__(self):
        self.capacity = 100 # Set the initial capacity of the hash table to 100.
        self.size = 0
        self.buckets = [None] * self.capacity # Initialize the hash table with None values.

    def _hash(self, key):
        # The hash function maps a key to a hash value.
        # In this implementation, we're using the modulo operator "%".
        hash_value = hash(key) % self.capacity
        return hash_value

    def insert(self, key, value):
        # Insert a key-value pair into the hash table.
        # If the key already exists, update its value.
        index = self._hash(key)
        if self.buckets[index] is None:
            # Create a new bucket if it's empty.
            self.buckets[index] = []
        for item in self.buckets[index]:
            if item[0] == key:
                # Update the value if the key already exists.
                item[1] = value
                return
        self.buckets[index].append([key, value])
        self.size += 1

    def search(self, key):
        # Search for a key in the hash table and return its value.
        index = self._hash(key)
        if self.buckets[index] is None:
            return None
        for item in self.buckets[index]:
            if item[0] == key:
                return item[1]
        return None

    def delete(self, key):
        # Delete a key from the hash table.
        index = self._hash(key)
        if self.buckets[index] is None:
            return
        for i in range(len(self.buckets[index])):
            if self.buckets[index][i][0] == key:
                self.buckets[index].pop(i)
                self.size -= 1
                return
