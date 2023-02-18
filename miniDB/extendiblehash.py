class Bucket:
    def __init__(self, local_depth, hash, size):
        '''
        Initialize a bucket with the given parameters.

        Args:
            local_depth: the depth of the bucket
            hash: the hash value of the bucket
            size: the size of the bucket
        '''
        self.local_depth = local_depth
        self.hash = hash
        self.size = size
        self.records = []

class Hash: 
    def __init__(self, bucket_size = 3, global_depth = 1, directory_size = 2):
        '''
        Initialize a hash table with the given parameters.

        Args:
            bucket_size: the size of each bucket
            global_depth: the depth of the hash table
            directory_size: the size of the directory
        '''
        self.bucket_size = bucket_size
        self.global_depth = global_depth
        self.directory = {}
        self.directory_size = directory_size
        self.directory = {i: Bucket(1, f"{i:b}", bucket_size) for i in range(2)}
        self.buckets_to_split = []

    def insert(self, record):
        '''
        Insert the given record into the hash table.

        Args:
            record: the record to be inserted
        '''
        key, idx = record
        pointer = int(self.binary_hash_value(key)[-self.global_depth:],2)
        if len(self.directory[pointer].records) < self.bucket_size:
            self.directory[pointer].records.append(record)
        else:
            if self.global_depth == self.directory[pointer].local_depth: 
                pointer = self.expand_directory(pointer,key)
                self.rehash_bucket()
            elif self.global_depth > self.directory[pointer].local_depth:
                pointer = self.bucket_split(pointer,key)
                self.rehash_bucket()
            self.directory[pointer].records.append(record)

    def find(self,key):
        '''
        Find the record with the given key.

        Args:
            key: the key to be found
        '''
        remainder_lsb = self.get_remainder(key)
        pointer = int(self.get_binary(remainder_lsb,self.global_depth)[-self.global_depth:],2)
        for record in self.directory[pointer].records:
            if key == record[0]:
                return record[1]#return index
        return -1

    def get_binary(self, key, depth):
        '''
        Convert the given key to a binary string of the given depth.

        Args:
            key: the key to be converted
            depth: the depth of the binary string
        '''
        return f"{key:0{depth}b}"

    def binary_hash_value(self, key):
        '''
        Convert the given key to a binary string of the global depth.

        Args:
            key: the key to be converted
        '''
        if isinstance(key, str):
            key = abs(sum([ord(c) for c in key]))
        return f"{key & ((1 << self.global_depth) - 1):b}"
    
    def get_remainder(self, key):
        '''
        Get the remainder of the given key.

        Args:
            key: the key to be converted
        '''

        if (isinstance(key, str)):
            key = abs(sum([ord(c) for c in key]))
        remainder_lsb = key % self.directory_size
        return remainder_lsb

    def rehash_bucket(self):
        '''
        Rehash the records in the bucket to be split.
        '''
        bucket = self.buckets_to_split
        bucket_records = bucket.records.copy()
        bucket.records.clear()
        for key, index in bucket_records:
            remainder_lsb = self.get_remainder(key)
            new_ptr = int(self.get_binary(remainder_lsb,self.global_depth)[-self.global_depth:],2)
            bucket = self.directory[new_ptr]
            bucket.records.append((key, index))
            bucket.hash = self.get_binary(remainder_lsb,self.global_depth)[-self.global_depth:]

    def bucket_split(self, pointer, key):
        '''
        Split the bucket at the given pointer.

        Args:
            pointer: the pointer to the bucket to be split
            key: the key to be inserted
        '''
        self.directory[pointer].local_depth += 1
        self.buckets_to_split = self.directory[pointer]
        remainder_lsb = self.get_remainder(key)
        key_in_binary = self.get_binary(remainder_lsb, self.global_depth -1)
        for i in [0, 1]:
            new_ptr = "{}".format(i) + key_in_binary[-(self.global_depth-1):][-self.global_depth:]
            new_ptr_int = int(new_ptr,2)
            new_bucket = Bucket(self.directory[pointer].local_depth,new_ptr,self.bucket_size)
            self.directory[new_ptr_int] = new_bucket
        remainder_lsb = self.get_remainder(key)
        new_ptr = int(key_in_binary[-self.global_depth:],2)
        return new_ptr

    def expand_directory(self, pointer, key):
        '''
        Expand the directory by doubling its size.
        
        Args:
            pointer: the pointer to the bucket to be split
            key: the key to be inserted
        '''
        self.global_depth += 1
        self.directory_size *= 2
        new_ptr = self.bucket_split(pointer,key)
        for key_ in self.directory.copy():
            bucket = self.directory[key_]
            if bucket.local_depth < self.global_depth:
                hash_ = bucket.hash
                for key in range(self.directory_size):
                    if self.directory.get(key) is None:
                        binary_ver = self.get_binary(key,bucket.local_depth)
                        if binary_ver[-bucket.local_depth:] == hash_: 
                            self.directory[key] = bucket
        return new_ptr
    
    def delete(self,key):
        remainder_lsb = self.get_remainder(key)
        pointer = int(self.get_binary(remainder_lsb,self.global_depth)[-self.global_depth:],2)
        for record in self.directory[pointer].records:
            if key == record[0]:
                self.directory[pointer].records.remove(record)
                if len(self.directory[pointer].records) == 0:
                    self.merge_bucket(pointer)
                return
        return -1