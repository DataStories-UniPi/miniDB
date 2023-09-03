class Bucket:
    def __init__(self):
        self.items = []
    
    def add(self, key, value):
        self.items.append((key, value))
    
    def get(self, key):
        for k, v in self.items:
            if k == key:
                return v
        raise KeyError(key)
    
    def remove(self, key):
        for item in self.items:
            if item[0] == key:
                self.items.remove(item)
                return
        raise KeyError(key)


class Hash:
    def __init__(self, size):
        self.size = size
        self.buckets = [Bucket() for _ in range(size)]

    def add(self, key, value):
        index = hash(key) % self.size
        self.buckets[index].add(key, value)
    
    def get(self, key):
        index = hash(key) % self.size
        return self.buckets[index].get(key)
    
    def remove(self, key):
        index = hash(key) % self.size
        self.buckets[index].remove(key)