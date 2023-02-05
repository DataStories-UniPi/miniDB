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
