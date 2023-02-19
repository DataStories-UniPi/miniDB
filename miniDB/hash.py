'''
https://gunet2.cs.unipi.gr/modules/document/file.php/TMC110/Lectures/05-FilesIndexing.pdf  Slide 20 (Extendible Hashing)
'''
import binascii # we will convert values to hex bytes and then take the modulo of the corresponding int with the key

class Hash:

    def __init__(self, b, key=2047): # 1st mersenne prime
        '''
        The hash index abstraction.
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
        if (len(selected.data)<selected.b):
            selected.data[value]=ptr
        else:
            self.split(selected,bits)
            self.insert(value,ptr) # recursive call after splitting


    def _search(self, value):
        '''
        Returns the bucket that the given value exists or should exist in.

        Args:
            value: float or string. The value being searched for.  
        '''
        # key to be hashed
        h_key=self.calc_hash(value)
        # formatted as binary value
        bin_val=format(h_key,'011b')
        bits=self.i_MSB(bin_val)       
        return self.hash_prefix[bits] # selected bucket (pointer)
    
    def find(self, value):
        '''
        Returns the pointer of the given value.

        Args:
            value: float or string. The value being searched for.  
        '''
        bucket=self._search(value)#the bucket that the given value exists or should exist in
        return bucket.find(value)
    
    def split(self, bucket_j,bits):
        '''
        Splits the bucket_j into two buckets and updates the hash_prefix dictionary
        It was implemented following the algorithm in the Database System Concepts,
        a Book by Avi Silberschatz, Henry F. Korth, and S. Sudarshan (https://www.db-book.com/ (pg. 1197-1203))

        Args:
            bucket_j: Bucket. The bucket to be split.
            bits: string. The first i bits of the hash value of the record that caused the split/The prefix that points to bucket_j.
        '''
        if self.i==bucket_j.i: # no pointer available for the new bucket

            self.i+=1 #increase hash prefix size (size is doubled)
            #replace each element of hash_prefix with 2 elements containing the same index as the original element
            new_prefix={}
            for key, value in self.hash_prefix.items():
                new_prefix[key+'0']=value
                new_prefix[key+'1']=value
            self.hash_prefix=new_prefix

            bucket_z=Bucket(self.b,self.i) #new bucket's prefix length is equal to i
            self.buckets.append(bucket_z) #add new bucket to the hash index
            self.hash_prefix[bits+'1']=bucket_z #the 2nd element that points to the bucket_j now points to the bucket_z

            bucket_j.i=self.i #bucket_j's prefix length is now equal to i
            
        elif self.i>bucket_j.i: # there is a pointer available for the new bucket
            bucket_j.i+=1 # update how many bits we are using
            bucket_z=Bucket(self.b,bucket_j.i)
            self.buckets.append(bucket_z)

            #find the lower half of hash prefixes/pointers that point to bucket_j and change them to point to bucket_z
            for prefix, ptr in self.hash_prefix.items():
                if prefix.startswith(bits[0:bucket_j.i]) and ptr==bucket_j:
                    self.hash_prefix[prefix]=bucket_z

        # apply the hash function to each record of bucket j and according to the first i bits,the record stays in bucket_j or moves to bucket_z
        j_new_data={} 

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
        '''
        Prints the hash index.

        Args:
            None
        '''
        print("i="+str(self.i))
        for prefix, bucket in self.hash_prefix.items():
            print("Prefix="+prefix+", i_bucket="+str(bucket.i)+" => "+str(bucket.data))
        
    
    def calc_hash(self,value):
        '''
        The hash function used to calculate the hash value of a given value.

        Args:
            value: float or string. The value to be hashed.
        '''
        if isinstance(value, str):
            value=int(binascii.hexlify(bytes(value,'utf-8')), 16)
        return value%self.key #we will get the MSB from this value

    def i_MSB(self,bin_value):
        '''
        returns the i MSB of a binary value 

        Args:
            bin_value: string. The binary value to be truncated.
        '''
        return bin_value[0:self.i]
        


class Bucket:
    '''
    Bucket abstraction.

    Explanation of the attribute i:
    Although i bits are required to find the correct bucket using the hash prefix dictionary, 
    several contiguous dictionary keys can point to the same bucket.
    All these keys will have a common prefix, but the length of this prefix can be less than i. 
    So,we associate an integer with each bucket that gives the length of that common hash prefix.
    '''
    def __init__(self, b,i):
        self.b = b # number of records held in block (blocking factor)
        self.i=i # number of bits used for hash prefix
        self.data = {} # dictionary of records and their pointers

    def find(self, value):
        '''
        Returns the pointer of the record with the given value.

        Args:
            value: float or string. The value of the record to be found.
        '''
        return self.data[value]