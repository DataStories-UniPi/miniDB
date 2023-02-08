'''
https://gunet2.cs.unipi.gr/modules/document/file.php/TMC110/Lectures/05-FilesIndexing.pdf  Slide 20 (Extendible Hashing)
'''
import binascii # we will convert values to hex bytes and then take the modulo of the corresponding int with the key

class Hash:
    key=2047 # 1st mersenne prime
    def __init__(self, b):
        '''
        The tree abstraction.
        '''
        self.b = b # blocking factor
        self.buckets = [] # list of nodes. Every new node is appended here

    def insert(self, value, ptr):
        '''
        Insert the value to the appropriate node.

        Args:
            value: float. The input value.
            ptr: float. The ptr of the inserted value (e.g. its index).
        '''
        arr=["dimitris","vassiliki","antonis"]
        # Converting String to integer
        for val in arr:
            num=int(binascii.hexlify(bytes(val,'utf-8')), 16)
            print(num)
            #convert back to string
            print(binascii.unhexlify(format(num, "x").encode("utf-8")).decode("utf-8"))
        
        h_key=num%key #we will get the MSB from this value

    def _search(self, value, return_ops=False):
        pass
    

    def split(self, node_id):
        # split buckets
        pass
    
    def show(self):
        # optionally, implement hash printing
        pass

class Bucket:
    def __init__(self, r):
        self.r = r # number of records held in block (blocking factor)
        self.values = [] if values is None else values # Values (the data from the pk column)
        self.ptrs = [] if ptrs is None else ptrs # ptrs (the indexes of each datapoint or the index of another bucket)