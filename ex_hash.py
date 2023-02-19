class Bucket:
    def __init__(self,b,lsb,hash_res) -> None:
        self.b=b
        self.known_lsb=lsb  #each bucket keeps the price of lsb either the point it was created or the last time it was split
        self.hash_res=hash_res
        self.data=[]
        self.overflown_elems=[]
    
    def insert(self,element,ptr):
        if (len(self.data)>=self.b):
            return False 
        self.data.append((element,ptr))
        return True 

class ExHash:
    def __init__(self,b) -> None:
        self.b=b
        self.lsb=1
        self.key=2047
        self.hash_prefix=dict([('0',Bucket(self.b,self.lsb,'0')),('1',Bucket(self.b,self.lsb,'1'))])
        self.overflown_elems=[] #When an overflow happens , elements are appended in a list so they can be organized later

    def insert(self,value,ptr):
        ''''
        The standard insert function , it gets the key for the given value and 
        either inserts the element in the right bucket , or appends it in overflown_elems
        and calls split() in order to reorganize the hash.
        '''
        pos=self.get_key(value)
        if not self.hash_prefix[pos].insert(value,ptr):
            self.overflown_elems.append((value,ptr))
            self.split()
    
    def insert_helper(self,val,ptr):
        '''
        This function is an internal one of the hash object,used only internally
        when overflows happens so that we dont have to call split recursively.

        '''
        pos=self.get_key(val)
        if not self.hash_prefix[pos].insert(val,ptr):
            self.overflown_elems.append((val,ptr))
        

    def get_key(self,val):
        '''
        Get the insert key (hash prefix key address) where val has to be inserted.
        We hash the value to get a unique represantation of it as an int.
        Then we perform the operation modulo 2047 with the hashed value and
        retrieve the self.lsb bits
        '''
        
        val=hash(val)  
        return bin(val%self.key).replace('0b','').zfill(32)[-self.lsb-1:-1]
    
    def split(self):
        '''
        This function is called when an overflow happens by the insert function.
        It checks whether the known lsb of an overflown bucket is smaller than the one of the hash and if its true ,
        it gets the address of the overflown bucket and creates two new buckets and two new keys for the hash prefix
        or if the lsb's are the same , it increases the lsb of the hash and reorganizes the hash_prefix dict.
        In each case , it inserts the overflown elements and the ones of overflown buckets in the hash.
        '''
        while len(self.overflown_elems)>0:  
            address=self.get_key(self.overflown_elems[0][0])
            overflownBucket = self.hash_prefix[address]
        
            if (overflownBucket.known_lsb<self.lsb):    #in this case , we dont need to increase the lsb,but only split the overflown bucket to two
                print("hello2")
                if address[0]=='1': #get the other address that this bucket is going to be slit
                    alt_address='0'+address[1:]
                else :
                    alt_address='1'+address[1:]

                self.hash_prefix[address]=Bucket(self.b,overflownBucket.known_lsb+1,address)
                self.hash_prefix[alt_address]=Bucket(self.b,overflownBucket.known_lsb+1,alt_address)
                self.insert_helper(self.overflown_elems[0][0],self.overflown_elems[0][1])
                self.overflown_elems.pop(0)

                for val in overflownBucket.data:    #insert the data of the overflown bucket again in the hash index
                    self.insert_helper(val[0],val[1])
                
                if len(address)<self.lsb:   #delete hash prefixes with less lsb's than the current one hold in self.lsb
                    del self.hash_prefix[address]
            else:   #in this case we have to increase the lsb and also change the hash prefix dictionary
                self.lsb+=1
                new_address1='0'+address    #create the new two address for the overflown bucket
                new_address2='1'+address
                for key in list(self.hash_prefix.keys()):
                    if key==address:    #in this case we already split the bucket and dont have to point those two new buckets
                        continue        #to the same bucket but in new separate ones
                    self.hash_prefix['0'+key]=self.hash_prefix[key] #point new hash prefixes to the same buckets based on lsb-1 bits
                    self.hash_prefix['1'+key]=self.hash_prefix[key] #i.e. 10 and 00 buckets have to point to bucket 0 (based on previous lsb)
                self.hash_prefix[new_address1]=Bucket(self.b,self.lsb,new_address1)
                self.hash_prefix[new_address2]=Bucket(self.b,self.lsb,new_address2)
                for key in list(self.hash_prefix.keys()):
                    if key==address:
                        continue
                    if len(key)<self.lsb:   #delete previous hash prefixes with smaller lsb length than the current one
                        del self.hash_prefix[key]
                self.insert_helper(self.overflown_elems[0][0],self.overflown_elems[0][1])
                self.overflown_elems.pop(0)
                for val in overflownBucket.data:
                    self.insert_helper(val[0],val[1])
                
                if len(address)<self.lsb:
                    del self.hash_prefix[address]

    def search(self,val):
        '''
        Get the key for the value given and either return the tuple (val,idx) if val is found,
        , or return None.idx is the index of the element val in the main file.
        '''
        key=self.get_key(val)
        for elem in self.hash_prefix[key].data:
            if val==elem[0]:
                return elem
        return None
    
    def print_hash(self):
        '''
        Used only for example purposes to show how the hash index works and saves values.
        Note that if two buckets have the same elements , it means that they are showing to the
        same bucket (considering we only insert unique values in the hash index)
        '''
        for key in self.hash_prefix:
            print(key,self.hash_prefix[key].data)