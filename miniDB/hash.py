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
        self.MSB = 1 # number of bits used for hash prefix
        # create as many buckets as hash prefix keys
        self.buckets = [Bucket(self.b) for i in range(2**self.MSB)] # list of buckets. Every value is placed by hashing function in one of these buckets
        for i in range(2**self.MSB):
            # dictionary keys are binary representations of MSB number range (0 to 2^msb-1)
            self.hash_prefix[format(i,'0'+str(self.MSB)+'b')]=self.buckets[i] # map hash to equivalent bucket


    def insert(self, value, ptr):
        # key to be hashed
        h_key=self.calc_hash(value)
        # formatted as binary value
        bin_val=format(h_key,'011b')
        bits=bin_val[0:self.MSB]
        selected=self.hash_prefix[bits] # selected bucket
        # Check if record fits in the bucket
        if (len(selected.data)<=selected.b):
            selected.data[value]=ptr
        else:
            pass # split()

    def _search(self, value, return_ops=False):
        pass
    

    def split(self, node_id):
        # split buckets
        pass # :(
    
    def show(self):
        # optionally, implement hash printing
        pass
    
    def calc_hash(self,value):
        if isinstance(value, str):
            value=int(binascii.hexlify(bytes(value,'utf-8')), 16)
        return value%self.key #we will get the MSB from this value


class Bucket: # kouvadaki
    def __init__(self, b):
        self.b = b # number of records held in block (blocking factor)
        self.data = {}