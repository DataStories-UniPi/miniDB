
class Bucket:
    '''
    Bucket Class
    is used by Hash class to create buckets 

    values in buckets are lists with the first index being the value and 2nd index a ptr to it

    '''
    def __init__(self, Depth):
        self.Local_depth = Depth
        self.Bucket_list = []

    def insert(self,value):
        '''
        Inserts item into the bucket
        value is a list that contains a value and a pointer
        '''
        if self.Local_depth == len(self.Bucket_list): # if the bucket is full
            return False # don't insert
        else:
            self.Bucket_list.append(value)
            return True

    def find(self, value):
        '''
        Find the value inside the bucket
        '''
        for i in self.Bucket_list: # iterating through bucket list to find the value
            if i[0] == value:
                return i[1]
        
        return False # if not found return False
        
    def remove(self, value):
        '''
        Removes the item with the value
        '''
        for i in self.Bucket_list: # iterating through bucket list to find the value
            if i[0] == value:
                self.Bucket_list.remove(i)
                return True
        
        return False # if not found return False

    def empty(self):
        '''
        Removes the elements from the bucket and returns them
        '''
        tempList = [] # creating a temporary list 
        for i in self.Bucket_list:
            tempList.append(i) # moving elements from bucket list to temporary list 
            self.Bucket_list.remove(i)

        return tempList # returning the temporary list

    def update_depth(self, newdepth):
        '''
        changes the Local depth of the Bucket
        '''
        self.Local_depth = newdepth

    def show(self):
        print(self.Bucket_list)



class Hash:
    '''
    Hash Class
    Extendible Hashing is done base on LSB.    
    Hashing can be done only in columns that have numeric values

    Hash dictionary keys will be strings.
    '''
    def __init__(self, Number_of_elements=1):

        # Number_of_elements represents the number of elements that we want to insert to the hash index first time
        # if not defined, we'll assume its 1

        self.Global_depth = 1 # number of bits that will be used for splitting buckets

        # Global_depth is the number of bits
        # h bits can represent 2^h numbers in decimal
        # so the number of buckets is 2**self.Global_depth
        # Local_depth (depth of buckets) is same as Global depth
        # the number of elements that can fit in the index is (2**self.Global_depth)*self.Global_depth
        
        # increasing Global depth until it can at least fit the amount of items 
        while (2**self.Global_depth)*self.Global_depth < Number_of_elements:
            self.Global_depth += 1                     
               

        self.Buckets = {}


        for i in range(2**self.Global_depth): 
            self.Buckets[bin(i)] = Bucket(self.Global_depth)

    def insert(self, value, index):
        '''
        Insert the value and its index to the appropriate bucket.
        First we turn the value to binary to find the bucket and then we add it
        If there is overflow, we call directory_expansion and try again        

        Args:
            value: float. The value we are inserting to the node.
            index: float. The ptr of the inserted value (e.g. its index).
        '''
        bucket_to_insert = bin(value % (2**self.Global_depth))

        insertion = self.Buckets[bucket_to_insert].insert([value,index])

        while not insertion: # while there is overflow, expand the directories
            self.directory_expansion()
            
            insertion = self.insert(value,index)

        return True
 
    def find(self, value):
        '''
        Returns the index of the value. If not found, returns None       

        Args:
            value: float. The value being searched for.
        '''

        bucket_to_look_for = bin(value % (2**self.Global_depth)) # finding the number of the bucket with mod and turning it into binary

        return self.Buckets[bucket_to_look_for].find(value) # searching the value in that bucket

    def remove(self, value):
        '''
        Removes the item with the value
        '''
        bucket_to_look_for = bin(value % (2**self.Global_depth))
        return self.Buckets[bucket_to_look_for].remove(value)

    def directory_expansion(self):
        '''
        When overflow happens, this method is called to split the buckets
        It gets all the elements out of the buckets 
        '''        

        current_buckets = 2**self.Global_depth
        self.Global_depth += 1 # increasing the global depth by 1
        buckets_needed = 2**self.Global_depth

        # for all existing buckets, increase local depth
        for i in range(current_buckets):
            self.Buckets[bin(i)].update_depth(self.Global_depth)

        # creating the new buckets
        for i in range(current_buckets, buckets_needed):
            self.Buckets[bin(i)] = Bucket(self.Global_depth)

        # emptying all buckets and rearranging the elements
        for i in range(current_buckets):
            tempList = self.Buckets[bin(i)].empty() # emptying the bucket and returning its elements
            for j in tempList:
                self.insert(j[0], j[1])
            
    def show(self):
        '''
        Print the values and relevant information.
        '''
        print('\nBuckets: \n')
        for i in range(2**self.Global_depth):
            print (format(i,'b').zfill(self.Global_depth) , end =" ")
            self.Buckets[bin(i)].show() # print the bucket


