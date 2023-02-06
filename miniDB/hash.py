import hashlib


#import libary hashlib
#Create class Hash
#Create initial state of hash index.
class Hash:
    def __init__(self, global_depth, max_bucket=4):
        '''
        The Hash abstraction.
        '''
        self.dict = {}
        self.buckets = []
        self.global_depth = global_depth
        self.bucket_max_size = max_bucket

        for i in range(pow(2, global_depth)):
            b = Bucket(global_depth)
            self.buckets.append(b)
            self.dict[i] = b
    #Show the constructed index
    def show(self):
        print("[", self.global_depth, "]")
        for i in range(pow(2, self.global_depth)):
            print("|| ", i, " || -> ", self.dict[i])

    #Create function hash insert, case 1:There is space in the bucket for a record
    def insert(self, value, ptr=-1):

        h = hash_func(value, pow(2, self.global_depth)) # dictionary cell
        b = self.dict[h]  # Find the right bucket from dictionary
        b.data.append({"value": value, "ptr": ptr})   # temporarily put the value in bucket

        if len(b.data) <= self.bucket_max_size: #If the bucket has space append the value and return
            return
        elif len(b.data) > self.bucket_max_size:  # if there is no more space in the bucket
            if b.local_depth == self.global_depth:  # if local_depth=global_depth
                self.global_depth += 1  # increase global depth
                b.local_depth = self.global_depth  # increase local depth
                for i in range(pow(2, self.global_depth - 1), pow(2, self.global_depth)):  # double directory
                    self.dict[i] = self.dict[i - pow(2, self.global_depth - 1)]

                nh = h + pow(2,self.global_depth - 1)# the cell that created the conflict points to a new bucket ( to nb )
                nb = Bucket(self.global_depth)
                self.buckets.append(nb)
                self.dict[nh] = nb

                b_data = b.data.copy()  # split the data to 2 buckets

                b.data = []

                for val in b_data:
                    hash_ = hash_func(val["value"], pow(2, self.global_depth))
                    self.dict[hash_].data.append(val)

                if len(b.data) >= self.bucket_max_size or len(nb.data) >= self.bucket_max_size:
                    try:
                        b.data.remove({"value": value, "ptr": ptr})
                    except:
                        pass
                    try:
                        nb.data.remove({"value": value, "ptr": ptr})
                    except:
                        pass

                    return self.insert(value, ptr)


#class for buckets creation
class Bucket:
    def __init__(self, local_depth):
        self.data = []
        self.local_depth = local_depth

    def __str__(self):
        return "[" + str(self.local_depth) + "] ... [" + ' '.join(str(e) for e in self.data) + "]"

#hash function
def hash_func(value, m):
    hash_object = hashlib.sha1(bytes(str(value), encoding='utf-8'))
    hex_dig = hash_object.hexdigest()
    res = int(hex_dig, 32)
    return res % m


