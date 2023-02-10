import mmh3

class Bucket:
    '''
    Bucket abstraction. Represents a bucket in the extendible hash index.
    '''
    def __init__(self, max_bucket_size):
        self.record = [] #the column with the index
        self.max_bucket_size = max_bucket_size #bucket size

    def insert(self, record):
        '''
        Insert a record into the bucket.
        '''
        self.record.append(record) #insert the record
        if len(self.record) > self.max_bucket_size: #check if the bucket is full, if it is return true
            return True
        return False

    def delete(self, record):
        '''
        Delete a record from the bucket.
        '''
        self.record.remove(record) #delete the record

# Define a hash function for the record
def hash_function(key, max_bucket_size):
    '''
    Returns the hash value of the record.
    '''
    key = str(key).encode()
    return mmh3.hash(key) % max_bucket_size

class ExtendibleHashIndex:
    def __init__(self, max_bucket_size):
        self.buckets = {}
        self.max_bucket_size = max_bucket_size

    def insert(self, record):
        '''
        Insert a record into the hash index.
        Create a new bucket if the hash value is not in the index.
        '''
        key, idx = record
        hash_value = hash_function(key, self.max_bucket_size)
        if hash_value not in self.buckets:
            self.buckets[hash_value] = Bucket(self.max_bucket_size) # Create a new bucket
        if self.buckets[hash_value].insert(record):
            self.split_bucket(hash_value)

    def delete(self, record):
        '''
        Delete a record from the hash index.
        '''
        key, idx = record
        hash_value = hash_function(key, self.max_bucket_size)
        if hash_value in self.buckets:
            self.buckets[hash_value].delete(record)

    def split_bucket(self, hash_value):
        '''
        Split a bucket into two buckets and add the values to the new buckets.
        '''
        original_bucket = self.buckets[hash_value]
        new_bucket_1 = Bucket(self.max_bucket_size)
        new_bucket_2 = Bucket(self.max_bucket_size)

        for record in original_bucket.record:
            key, idx = record
            new_hash_value = hash_function(key, self.max_bucket_size * 2)
            if new_hash_value & 1:
                new_bucket_1.insert(record)
            else:
                new_bucket_2.insert(record)

        self.buckets[hash_value] = new_bucket_1
        self.buckets[hash_value + self.max_bucket_size] = new_bucket_2

    def find(self, key):
        '''
        Find a record in the hash index.
        '''
        hash_value = hash_function(key, self.max_bucket_size)
        if hash_value in self.buckets:
            bucket = self.buckets[hash_value]
            for record in bucket.record:
                if record[0] == key:
                    return record[1]
        return None