class Bucket:
    def __init__(self):
        self.records = []
    
    def insert(self, record):
        self.records.append(record)
    
    def search(self, key):
        for record in self.records:
            if record[0] == key:
                return record
        return None
    
class ExtendibleHash:
    def __init__(self, num_records, bucket_size=2):
        self.bucket_size = bucket_size
        self.global_depth = 1
        while (2 ** self.global_depth) < num_records:
            self.global_depth += 1
        self.directory = [Bucket() for i in range(2 ** self.global_depth)]
        self.bucket_size = (num_records + len(self.directory) - 1) // len(self.directory)
        
    def hash(self, key): # hash function
        return hash(key) % (2 ** self.global_depth)
    
    def insert(self, record):
        key, value = record
        bucket = self.directory[self.hash(key)]
        if len(bucket.records) < self.bucket_size:
            bucket.insert(record)
        else:
            size = len(bucket.records)
            self.split(bucket)
            if len(bucket.records) > size:
                bucket.insert(record)
    
    def split(self, bucket):
        self.global_depth += 1
        new_directory = [Bucket() for i in range(2 ** self.global_depth)]
        for record in bucket.records:
            key, value = record
            if self.hash(key) >= len(new_directory) // 2:
                new_directory[self.hash(key) - len(new_directory) // 2].insert(record)
            else:
                new_directory[self.hash(key)].insert(record)
        self.directory = new_directory
        bucket.records = []
    
    def search(self, key):
        return self.directory[self.hash(key)].search(key)
