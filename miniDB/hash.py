'''
https://gunet2.cs.unipi.gr/modules/document/file.php/TMC110/Lectures/05-FilesIndexing.pdf  Slide 20 (Extendible Hashing)
'''
import binascii # we will convert values to hex bytes and then take the modulo of the corresponding int with the key

class Hash:

    def __init__(self, b, key=2047): # 1st mersenne prime
        '''
        The tree abstraction.
        '''
        self.b = b # blocking factor
        self.hash_prefix = {} # search dictionary
        self.key = key # hash key
        self.i = 1 # number of bits used for hash prefix
        # create as many buckets as hash prefix keys
        self.buckets = [Bucket(self.b,self.i) for i in range(2**self.i)] # list of buckets. Every value is placed by hashing function in one of these buckets
        for j in range(2**self.i):
            # dictionary keys are binary representations of MSB number range (0 to 2^msb-1)
            self.hash_prefix[format(j,'0'+str(self.i)+'b')]=self.buckets[j] # map hash to equivalent bucket


    def insert(self, value, ptr):
        # key to be hashed
        h_key=self.calc_hash(value)

        # formatted as binary value
        bin_val=format(h_key,'011b')
        bits=self.i_MSB(bin_val)
        selected=self.hash_prefix[bits] # selected bucket (pointer)
        # Check if record fits in the bucket
        if (len(selected.data)<=selected.b):
            selected.data[value]=ptr
        else:
            self.split(selected,bits)
            self.insert(value,ptr) # recursive call after splitting


    def _search(self, value, return_ops=False):
        pass
    

    def split(self, bucket_j,bits):
        '''
        https://www.db-book.com/ (pg. 1197-1203)
        '''
        print("hi")
        if self.i==bucket_j.i_bucket: # no pointer available for the new bucket

            self.i+=1 #increase hash prefix size (size is doubled)
            #replace each element of hash_prefix with 2 elements containing the same index as the original element
            new_prefix={}
            for key, value in self.hash_prefix.items():
                new_prefix[key+'0']=value
                new_prefix[key+'1']=value
            self.hash_prefix=new_prefix

            bucket_z=Bucket(self.b,self.i) #new bucket's prefix length is equal to i
            self.buckets.append(bucket_z) #add new bucket to the hash index
            self.hash_prefix[bits+'1']=len(self.buckets)-1 #the 2nd element that points to the bucket_j now points to the bucket_z

            bucket_j.i_bucket=self.i #bucket_j's prefix length is now equal to i
            
        elif self.i>bucket_j.i_bucket: # there is a pointer available for the new bucket
            bucket_j.i_bucket+=1 # update how many bits we are using
            bucket_z=Bucket(self.b,bucket_j.i_bucket)
            list_prefixes=[]
            for prefix, ptr in self.hash_prefix.items():
                if ptr == bucket_j:
                    list_prefixes.append(prefix)
                    print(ptr)
            for i in range(len(list_prefixes)//2,len(list_prefixes)): # half the pointers point to the new bucket

                 self.hash_prefix[list_prefixes[i]]=len(self.buckets)-1
 
        # apply the hash function to each record of bucket j and according to the first i bits,the record stays in bucket_j or moves to bucket_z
        j_new_data={} 
        print(type(bucket_j))
        print(type(bucket_j.data))

        for r_key,r_value in bucket_j.data.items():
                
            # key to be hashed
            h_key=self.calc_hash(r_key)
            # formatted as binary value
            bin_val=format(h_key,'011b')
            bits=self.i_MSB(bin_val)
                
            bucket_insert=self.hash_prefix[bits] 
            if bucket_insert==bucket_j:
                j_new_data[r_key]=r_value
            else:
                bucket_z.data[r_key]=r_value

        bucket_j.data=j_new_data
       
    
    def show(self):
        # optionally, implement hash printing
        pass
    
    def calc_hash(self,value):
        '''
        
        '''
        if isinstance(value, str):
            value=int(binascii.hexlify(bytes(value,'utf-8')), 16)
        return value%self.key #we will get the MSB from this value

    def i_MSB(self,bin_value):
        '''
        returns the i MSB of a binary value 
        '''
        return bin_value[0:self.i]
        


class Bucket: # kouvadaki
    def __init__(self, b,i_bucket):
        self.b = b # number of records held in block (blocking factor)
        self.i_bucket=i_bucket
        self.data = {}