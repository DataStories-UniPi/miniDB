BUCKET_SIZE = 20 # size of internal array in bucket

class Bucket:
    def __init__(self):
        self.map = []
        self.local_depth_size = 0

    def insert(self, k, v):
        for i, (key, value) in enumerate(self.map):
            if key == k:
                del self.map[i]
                break
        self.map.append((k, v))

    def get(self, k):
        for key, value in self.map:
            if key == k:
                return value

    def get_MSB(self):
      return 1 << self.local_depth # MSB variant for extendible hashing

class HashTable:
    def __init__(self):
        self.global_depth_size = 0
        self.directory = [Bucket()]
        self.capacity = BUCKET_SIZE

    def get_bucket(self, k):
        h = hash(k)
        return self.directory[h & ((1 << self.global_depth_size) - 1)] # shift left by global_depth_size bits

    def hash(self, key):
        hashsum = 0
        for idx, c in enumerate(key):
            hashsum += (idx + len(key)) ** ord(c)
            hashsum = hashsum % self.capacity # hash function based on the modulo operator
        return hashsum

    def insert(self, key, value):
        bucket = Bucket() # create new bucket object
        b = self.get_bucket(key) # we hash the key and search the bucket which has that key
        
        b.insert(key, value) # insert the new value with the key to the bucket
        if len(bucket.map) >= BUCKET_SIZE: # change the size dynamically 
            if b.local_depth_size == self.global_depth_size:
                self.directory *= 2
                self.global_depth_size += 1

            b0 = Bucket() # split into two bucket objects
            b1 = Bucket()
            b0.local_depth_size = b1.local_depth_size = b.local_depth_size + 1
            MSB = b.get_MSB() # get MSB variant
            for key2, value2 in b.map:
                h = hash(key2)
                new_b = b1 if h & MSB else b0
                new_b.insert(key2, value2)

            for i in range(hash(key) & (MSB - 1), len(self.directory), MSB):
                self.directory[i] = b1 if i & MSB else b0

    def get(self, k):
        return self.get_bucket(k).get(k)


