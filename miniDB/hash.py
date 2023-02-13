
#Bucket size value can be changed here.
BUCKET_SIZE = 3

#Bucket class which keeps the items.
class Bucket:
    def __init__(self, local_depth, size, hash ):
        self.local_depth = local_depth
        self.size = size
        self.items = []
        self.hash = hash
    
# Extendible Hash table class.
class Extendible_Hash: 
    def __init__(self, bucket_size):
        
        # Global depth starts at one, so we start with two buckets.
        self.global_depth = 1
        self.bucket_size = bucket_size
        bucket_zero = Bucket(1,bucket_size,"{0:b}".format(0))
        bucket_one = Bucket(1,bucket_size,"{0:b}".format(1))
        self.size = 2
        self.b2s = [] #B2s is the bucket to split, when we need to split.

        #Using a python dic to store the buckets.
        dic = {0: bucket_zero, 1: bucket_one}
        
        self.dic = dic

    #Getting binary format of a number, along with the number of bits we need.
    def get_bin(self,num,depth):
        return "{:0{}b}".format(num,depth)

    #Check if the bucket has room for another value.
    def bucket_check(self, pointer):
        get_bucket = self.dic[pointer]
        if len(get_bucket.items) < self.bucket_size:
            return True
        else:
            return False

    #Rehashing the items in a bucket after splitting.
    def rehash(self, pointer):
        get_bucket = self.b2s
        bucket_items = get_bucket.items.copy()
        get_bucket.items.clear()
        for item,idx in bucket_items:
            mod = item % self.size
            binary_ver = self.get_bin(mod,self.global_depth)
            pointer_ = int(binary_ver[-self.global_depth:],2)
            get_bucket = self.dic[pointer_]
            get_bucket.items.append((item,idx))
            get_bucket.hash = binary_ver[-self.global_depth:]
    
    #Splitting a bucket in two new ones.
    def bucket_split(self, pointer, num):
        
        get_bucket = self.dic[pointer]
        get_bucket.local_depth += 1
        self.b2s = get_bucket

        mod = num % self.size
        binary_ver = self.get_bin(mod,self.global_depth-1)
        binary_ver = binary_ver[-(self.global_depth-1):]
        
        first_pointer = "0" + binary_ver[-self.global_depth:]
        second_pointer = "1" + binary_ver[-self.global_depth:]
        
        pointer_f_int = int(first_pointer,2)
        pointer_s_int = int(second_pointer,2)
        
        new_bucket_f = Bucket(get_bucket.local_depth,self.bucket_size,first_pointer)
        self.dic[pointer_f_int] = new_bucket_f

        new_bucket_s = Bucket(get_bucket.local_depth,self.bucket_size,second_pointer)
        self.dic[pointer_s_int] = new_bucket_s

        mod = num % self.size
        binary_ver = self.get_bin(mod,self.global_depth-1)
        pointer_ = int(binary_ver[-self.global_depth:],2)
    
        return pointer_

    # Doubling the directory.
    def directory_expansion(self, pointer, num):
        self.global_depth += 1
        self.size *= 2
        
        new_pointer = self.bucket_split(pointer,num)

        #This for loop is needed in order to assign the 'dangling' pointers of the newly expanded table to already existing buckets.
        for key_ in self.dic.copy():
            bucket = self.dic[key_]
            if bucket.local_depth < self.global_depth:
                hash_ = bucket.hash
                for key in range(self.size):
                    if self.dic.get(key) is None:
                        binary_ver = self.get_bin(key,bucket.local_depth)
                        if binary_ver[-bucket.local_depth:] == hash_: 
                            self.dic[key] = bucket
        
        return new_pointer

    #Inserting a number and its index in the table.
    def insert(self,num,idx):

        mod = num % self.size
        binary_ver = self.get_bin(mod,self.global_depth)
        pointer = int(binary_ver[-self.global_depth:],2)

        fit_check = self.bucket_check(pointer)

        #There is space in the bucket, so add it.
        if fit_check:
            get_bucket = self.dic[pointer]
            get_bucket.items.append((num,idx))
        else:
            #Otherwise, keep doubling the directory and/or splitting buckets until there is room for the new item.
            
            current_key = pointer
            while fit_check == False:
                get_bucket = self.dic[current_key]

                #Expand the directory if the depths are equal
                if self.global_depth == get_bucket.local_depth: 
                    hold_prev = current_key
                    current_key = self.directory_expansion(current_key,num)
                    self.rehash(hold_prev)
                    fit_check = self.bucket_check(current_key)    
                
                #Otherwise just split the bucket.
                elif self.global_depth > get_bucket.local_depth:
                    hold_prev = current_key
                    current_key = self.bucket_split(current_key,num)
                    self.rehash(hold_prev)
                    fit_check = self.bucket_check(current_key)
            
            #Adding the item when we get the pointer we need.
            get_bucket = self.dic[current_key]
            get_bucket.items.append((num,idx))

    #Show the id of each pointer and the list of items in the bucket. 
    def show(self):
        self.dic = dict(sorted(self.dic.items()))
        for key in self.dic:
            print(key, ":",  self.dic[key].items)
    
    #Find an item in the table and return its index, otherwise return -1
    def find(self,num):
        
        mod = num % self.size
        binary_ver = self.get_bin(mod,self.global_depth)
        pointer = int(binary_ver[-self.global_depth:],2)
        get_bucket = self.dic[pointer]

        get_row = -1
        for item, idx in get_bucket.items:
            if num == item:
                get_row = idx
                break

        return [get_row]


#test main function
if __name__== "__main__" :
    
    e_hash = Extendible_Hash(BUCKET_SIZE)
    for idx,num in enumerate(range(100,200)):
        e_hash.insert(num,idx)

    e_hash.show()
    print(e_hash.find(127))

