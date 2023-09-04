class ExtendibleHashing:
    def __init__(self, global_depth):
        self.global_depth = global_depth
        self.directory = {}
        self.bucket_size = 4  # Number of elements in each bucket

    def hash_function(self, key):
        
        hash_value = 0
        for char in key:
            hash_value = (hash_value * 31 + ord(char)) % (2 ** self.global_depth)
        return hash_value

    def insert(self, key, value):
        hashed_key = self.hash_function(value)
        if hashed_key in self.directory:
            bucket = self.directory[hashed_key]
            for i, (k, v) in enumerate(bucket):
                if k == value:
                    bucket[i] = (k, value)  # Update value for existing key
                    return
            if len(bucket) < self.bucket_size:
                bucket.append((key, value))
            else:
                if self.global_depth == len(bin(hashed_key)) - 2:
                    self.double_directory()
                self.split_bucket(hashed_key)
                self.insert(key, value)
        else:
            if self.global_depth == len(bin(hashed_key)) - 2:
                self.double_directory()
            self.directory[hashed_key] = [(key, value)]


    def double_directory(self):
        self.global_depth += 1
        directory_size = 2 ** (self.global_depth - 1)
        for i in range(directory_size):
            if i in self.directory:
                self.directory[i + directory_size] = self.directory[i]
            else:
                self.directory[i + directory_size] = []

    def split_bucket(self, hashed_key):
        bucket = self.directory[hashed_key]
        new_bucket = []
        split_index = len(bucket) // 2

        # Split the bucket into two by creating a new bucket and updating the directory
        new_bucket = bucket[split_index:]
        bucket = bucket[:split_index]
        self.directory[hashed_key] = bucket

        # Update the hashed keys of the new bucket and its duplicates
        new_hashed_key = hashed_key + (2 ** (self.global_depth - 1))
        for key in range(new_hashed_key, new_hashed_key + (2 ** (self.global_depth - 1)), 2 ** (self.global_depth - 1)):
            self.directory[key] = new_bucket.copy()




    def find(self, key):
        self.global_depth=1
        hashed_key = self.hash_function(key)
        if hashed_key in self.directory:
            bucket = self.directory[hashed_key]
            for item in bucket:
                if item[1] == key:
                    return item[0]
        return None

    def delete(self, key):
        hashed_key = self.hash_function(key)
        if hashed_key in self.directory:
            bucket = self.directory[hashed_key]
            for i, item in enumerate(bucket):
                if item[0] == key:
                    del bucket[i]
                    return True
        return False

    def show(self):
        for hashed_key, bucket in self.directory.items():
            print(f"Hashed Key: {hashed_key}")
            for item in bucket:
                print(f"  Key: {item[0]}, Value: {item[1]}")

