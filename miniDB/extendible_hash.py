import numbers
#from faker import Faker


class ExtendibleHash:
    '''
        Creates an ExtendibleHash object.
        By default it uses LSB method.
        This can be changed by passing string 'msb' into the constructor.
        
        Args:
            method: a string, either 'lsb', or 'msb' options are available
            bucket_depth: an int, the number of values a bucket can consist of before splitting
    '''
    def __init__(self, method='lsb', bucket_depth=3):
        method = method.lower()
        
        self.__no_of_bits = 1       # number of bits used as keys (potentialy changes during insertions)
        self.__bucket_depth = bucket_depth     # maximum number of values a bucket can hold (this doens't change during insertions)
        self.__method = method      # method used to hash values in combination with self.__hash__
        
        if self.__method not in ['lsb', 'msb']:
            raise ValueError('method must be either lsb or msb')
        
        self.data = dict()
        self.data[format(0, 'b')] = []
        self.data[format(1, 'b')] = []
        
        
    '''
        Gets called after each insert operation.
        Readjusts the self.data if the number of values in a bucket exceeds self.__bucket_depth.
        - Copies the data into a list.
        - Increases the number of bits used as keys by 1.
        - Clears the self.data obj.
        - Creates the new keys.
        - Rehashes the old values into adjusted self.data.
        
        Args:
            bucket_let: an int, the number of values stored in the bucket, 
            if it exceeds bucket_depth we need to split, otherwise nothing happens
    '''
    def __readjust(self, bucket_len):
        if bucket_len > self.__bucket_depth:
            old_data = list(self.data.values())
            self.__no_of_bits += 1
            
            self.data.clear()
            
            for i in range(2 ** self.__no_of_bits):
                id = self.__binary_value(i)
                self.data[id] = []

            for bucket in old_data:
                for l in bucket:
                    self.insert(l[0], l[1])
    

    '''
        Returns the binary representation of given value.
        Args:
            value: any basic numeric type(int, float) or a string.
    '''
    def __binary_value(self, value):
        rv = ''
        
        if isinstance(value, numbers.Number):
            rv = format(value, 'b')
        elif isinstance(value, str):
            rv = ''.join(format(ord(x), 'b') for x in value)
        else:
            raise TypeError
        
        # if number of bits used as keys is greater than the number of bits of given value:
        # add '0's (diff zeros) at the front
        diff = self.__no_of_bits - len(rv)
        if diff > 0: rv = '0' * diff + rv
        return rv            
    
    '''
        Hashes the given value
        
        Args:
            value: any basic numeric type(int, float) or a string.
    '''
    def __hash_value(self, value):
        if isinstance(value, str):
            return (int(''.join(format(ord(x), 'b') for x in value), 2)) % 64       # string -> binary string -> int -> mod 64
        return value % 64
        
    
    '''
        Inserts value into self.data , value consists of [actual_value, row_index] actual_value must be either numeric or of type str.
        - Converts given value into binary.
        - Finds which bucket it must be inserted to using the last self.__number_of_bits bits as key.
        - Calls self.__readjust__ to check for overflows and readjust the data if need be.
        
        Args:
            value: any basic numeric type(int, float) or a string.
            idx: an int, the index of the value(record index)
    '''
    def insert(self, value, idx):
        hashed_value = self.__hash_value(value)
        bin_value = self.__binary_value(hashed_value)
        value_id = bin_value[-self.__no_of_bits:]
        self.data[value_id].append([value, idx])
        self.__readjust(len(self.data[value_id]))


    '''
        Finds the bucket that given value is stored in, value must be either numberic or of type str.
        If the value is indeed stored in self.data, returns tuple (True, the bucket that its stored in(list of values within bucket)).
        If the value is not stored in self.data, returns tuple (False, None).
    '''
    def contains(self, value):
        hashed_value = self.__hash_value(value)
        bin_value = self.__binary_value(hashed_value)
        value_id = None
        
        if self.__method == 'lsb':
            value_id = bin_value[-self.__no_of_bits:]
        else:
            value_id = bin_value[:self.__no_of_bits]
        
        if value_id in list(self.data.keys()):
            return True, self.data[value_id]
        return False, None


    '''
        Finds the bucket that given value is stored in, value must be either numberic or of type str.
        If the value is indeed stored in self.data, returns a list withe the indexes that was given when the value was inserted
        If the value is not stored in self.data, returns empty list.
        If get_duplicates==False and the value is within self.data the returned list will consist of a single element
        
        Args:
            value: any basic numeric type(int, float) or a string.
            get_duplicates: a boolean, set to True if duplicates are allowed, default is False            
    '''
    def find(self, value, get_duplicates=False):
        hashed_value = self.__hash_value(value)
        bin_value = self.__binary_value(hashed_value)
        value_id = None
        
        if self.__method == 'lsb':
            value_id = bin_value[-self.__no_of_bits:]
        else:
            value_id = bin_value[:self.__no_of_bits]
        
        rows = []
        if value_id in list(self.data.keys()):
            bucket = self.data[value_id]
            for l in bucket:
                if l[0] == value:
                    if not get_duplicates:
                        return [l[1]]
                    else:
                        rows.append(l[1])
        return rows


##faker = Faker()
##names = [faker.unique.first_name() for _ in range(10)]
##for name in names:
##    hash.insert(name)
#l = [28, 4, 19, 1, 22, 16, 12, 0, 5, 7]
#for i in range(len(l)):
#    hash.insert(l[i], i)

#print(hash.data)

#for value in l:
#    print(value, hash.find(value))