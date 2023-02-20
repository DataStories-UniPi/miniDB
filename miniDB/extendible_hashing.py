from pprint import pprint
import hashlib

class ExtendibleHashing:
    '''
    A class that implements an LSB (Least Significant Bit) extendible hashing data structure.
    '''
    def __init__(self, bits=1, bucket_size=3):
        '''
        Initializes the hash table with a given number of bits and a given bucket size.
        
        args:
            <> bits: The number of bits used to index the buckets of the hash table.
            <> bucket_size: The maximum number of records that can be stored in a bucket.
        '''
        self.bits = bits # number of bits used to index the buckets of the hash table.
        self.buckets = {i: [] for i in range(2 ** self.bits)} # initializing the bucket with 2 ** depth empty buckets.
        self.bucket_size = bucket_size # maximum number of records that can be stored in a bucket.

    def _add(self, key, value):
        '''
        Adds a key-value pair to the hash table.
        
        Args:
            <> key: The object to be hashed.
            <> value: The object to be stored in the hash table.
        '''
        lsb = self._hash(key) # the LSB (least significant bits) is the hash value of the key.
        bucket = self.buckets[lsb]
        if len(bucket) < self.bucket_size:
            bucket.append((key, value))
        else:
            self._split(bucket.copy(), lsb)
            self._add(key, value)

    def _remove(self, key):
        '''
        Removes a key-value pair from the hash table.
        Returns True if the key-value pair was removed successfully, False otherwise.
        
        Args:
            <> key: The object which is used to find the key-value pair to be removed.
        '''
        lsb = self._hash(key) # the LSB (least significant bits) is the hash value of the key.
        bucket = self.buckets[lsb]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                del bucket[i]
                return True
        return False
    
    def _get(self, key):
        '''
        Returns the value associated with the given key, or None if the key is not found.
        
        Args:
            <> key: The object which is used to find the key-value pair.
        '''
        lsb = self._hash(key) # the LSB (least significant bits) is the hash value of the key.
        bucket = self.buckets[lsb]
        for k, v in bucket:
            if k == key:
                return v
        return None
    
    def _hash(self, key):
        '''
        Returns an integer that represents the index of the bucket where
        the key-value pair with the given key should be stored in the hash table.
        
        Args:
            <> key: The object to be hashed.
            
        Notes:
            <> hashlib.sha256 is used to be sure that the hash value for the specified key
            is unique and will be the same for every execution of the program.
        '''
        value = int(hashlib.sha256(str(key).encode()).hexdigest(), 16)
        return value % len(self.buckets)
    
    def _split(self, bucket, lsb):
        '''
        Splits the bucket with the given LSB (least significant bits) into 2^(bits + 1) buckets.
        
        Args:
            <> bucket: The bucket to be split.
        '''
        self.bits += 1
        temp_buckets = {i: [] for i in range(2 ** self.bits)}
        for key in self.buckets.keys():
            if key == lsb:
                continue
            temp_buckets[key] = self.buckets[key]
        self.buckets = temp_buckets
        for k, v in bucket:
            self._add(k, v)
    
    def _print(self):
        '''
        Prints the hash table.
        '''
        pprint(self.buckets)