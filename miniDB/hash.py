class Hash:

    def __init__(self):
        # create an empty dictionary to store the hash index
        self.hash_index = {}

    def create_hash_index(self, value):
        # create a hash value for the record using the built-in hash function
        hash_value = hash(str(value))

        # add the hash value as the key and the record as the value in the dictionary
        self.hash_index[hash_value] = value

        return self.hash_index
