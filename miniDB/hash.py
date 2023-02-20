class Hash:
    def __init__(self):
        self.hash_table = {}

    def hash_func(self, key):
        return sum(ord(c) for c in key)

    def find(self, key):
        hash_key = self.hash_func(key)
        if hash_key in self.hash_table:
            for k, v in self.hash_table[hash_key]:
                if k == key:
                    return v
        return None

    def insert(self, key, value):
        hash_key = self.hash_func(key)
        if hash_key not in self.hash_table:
            self.hash_table[hash_key] = []
        for i, (k, v) in enumerate(self.hash_table[hash_key]):
            if k == key:
                self.hash_table[hash_key][i] = (key, value)
                return
        self.hash_table[hash_key].append((key, value))
