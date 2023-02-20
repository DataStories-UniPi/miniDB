from pprint import pprint


class ExtendibleHashing:
    '''
    A class that implements an extendible hashing data structure.
    '''
    def __init__(self, bits=0, bucket_size=3):
        '''
        Initializes the hash table with a given number of bits and a given bucket size.
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
            
        Notes:
            <> We use the 'zfill' method to pad the binary string with zeros to the left until it has n bits,
            because we have to be sure that the binary string is always greater than or equal to the depth of the hash table.
        '''
        h = self._hash(key) # get the hash value of the key.
        n = self.bits + 1 # n is the maximum number of bits that can be used to index the buckets of the hash table.
        binary_string = bin(h)[2:].zfill(n) # convert the hash value to a binary string with n bits.
        msb = binary_string[:self.bits] # extract the MSB (most significant bits).
        msb = int(msb, 2) # convert the MSB to an integer (from binary to decimal).
        bucket = self.buckets[msb]
        if len(bucket) < self.bucket_size:
            bucket.append((key, value))
        else:
            self._split(bucket.copy(), msb)
            self._add(key, value)

    def _remove(self, key):
        '''
        Removes a key-value pair from the hash table.
        Returns True if the key-value pair was removed successfully, False otherwise.
        
        Args:
            <> key: The object which is used to find the key-value pair to be removed.
        '''
        h = self._hash(key) # get the hash value of the key.
        n = self.bits + 1 # n is the maximum number of bits that can be used to index the buckets of the hash table.
        binary_string = bin(h)[2:].zfill(n) # convert the hash value to a binary string with n bits.
        msb = binary_string[:self.bits] # extract the MSB (most significant bits).
        msb = int(msb, 2) # convert the MSB to an integer (from binary to decimal).
        bucket = self.buckets[msb]
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
        h = self._hash(key) # get the hash value of the key.
        n = self.bits + 1 # n is the maximum number of bits that can be used to index the buckets of the hash table.
        binary_string = bin(h)[2:].zfill(n) # convert the hash value to a binary string with n bits.
        msb = binary_string[:self.bits] # extract the MSB (most significant bits).
        msb = int(msb, 2) # convert the MSB to an integer (from binary to decimal).
        bucket = self.buckets[msb]
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
            <> The 'hash' built-in function returns a hash value for the specified object.
        '''
        return hash(key) % len(self.buckets)
    
    def _split(self, bucket, msb):
        '''
        Splits the bucket with the given MSB (most significant bits) into 2^(bits + 1) buckets.
        
        Args:
            <> bucket: The bucket to be split.
        '''
        self.bits += 1
        temp_buckets = {i: [] for i in range(2 ** self.bits)}
        for key in self.buckets.keys():
            if key == msb:
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

if __name__ == '__main__':
    # Create an ExtendibleHashing object with a bucket size of 2.
    hash_table = ExtendibleHashing(1)

    # Add some key-value pairs to the hash table.
    hash_table._add(1, "one")
    hash_table._add(2, "two")
    hash_table._add(3, "three")
    hash_table._add(4, "four")
    hash_table._add(5, "five")

    # Lookup some values by key.
    print(hash_table._get(1)) # Output: one
    print(hash_table._get(2)) # Output: two
    print(hash_table._get(3)) # Output: three
    print(hash_table._get(4)) # Output: four
    print(hash_table._get(5)) # Output: five

    # Remove a key-value pair from the hash table.
    hash_table._remove(2)

    # Print the hash table.
    hash_table._print()
