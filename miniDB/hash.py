import math
class Hash:
    def __init__(self):
     self.capacity=3 #bucket capacity
     self.global_depth=1 
     bucket1=Bucket(bucket=[],ld=1)
     bucket2=Bucket(bucket=[],ld=1)
     self.data={'0':bucket1,'1':bucket2}

 
    def get_hash_index(self,key):
      '''
       Hash function.Returns the hashed value.

       key:The key used for placing the tuple in the correct bucket according to a hash function.
      '''
      if type(key)==int:
        h=key
      elif type(key)==float:
        h=math.ceil(key)
      else: # type string
        h=0
        for c in key:
          h+=ord(c) # ascii number
      size=2**(self.global_depth) 
      hash=h%(size) #returns number of lsb
      hashed=int(bin(hash)[2:])
      hash_index=str(hashed)
      if(self.global_depth>1): 
       if(len(hash_index)!=self.global_depth):
         hash_index=hash_index.zfill(self.global_depth) #fills with zeros
      return hash_index
   
    def insert(self,key,value):
      '''
        Insert the key and its value(pointer) to the appropriate bucket.
        Args:
            key:The key used for placing the tuple in the correct bucket.
            value: int. The ptr of the inserted value (e.g. its index).
      '''
      hash_key = str(self.get_hash_index(key))
      found_key = False

      for record in enumerate(self.data):
        record_key, record_val = record
        if record_key == key and record_val==value:
          found_key = True
          break     
      if not found_key:
        if len(self.data[hash_key].bucket)==self.capacity: #full bucket
          self.split_bucket(key,hash_key,value)  # call split function         
        else: # no split -> add tuple to bucket
          self.data[hash_key].bucket.append((key,value))


    def split_bucket(self,key,hash_key,value):
     '''
      Checks the global depth in relation to the local depth.If the global depth is equal to the local depth
      then directory expansion,rehashing of the bucket and increment by one of the local and global depth occur.If the
      local depth is less than the global depth then only rehashing of the bucket and increment by one of the local depth occur.

      Args:
        key:The key used for placing the tuple in the correct bucket.
        hash_key:str.The hashed key where the bucket overflow occurs.
        value: int. The ptr of the inserted value (e.g. its index).

     '''
     list1=[]  
     for key1,value1 in(self.data[hash_key].bucket):   
         list1.append((key1,value1))

     if((key,value)) not in list1:
       list1.append((key,value))  
     if self.global_depth==self.data[hash_key].ld:   # global depth = local depth    
        self.global_depth+=1
        self.directory_expansion(hash_key)
        self.rehashing(list1)
     elif self.data[hash_key].ld<self.global_depth:  # local depth < global depth
        h=self.data[hash_key].ld
        key2=hash_key[-h:]
        for key in (self.data):
          if self.data[hash_key].bucket==self.data[key].bucket and len(key)==len(hash_key) and key!=hash_key and key2==key[-h:]: # keys point to the same bucket
             self.data[key].bucket=[]
             self.data[key].ld+=1
        self.data[hash_key].bucket=[]
        self.data[hash_key].ld+=1
        self.rehashing(list1)
     
    def directory_expansion(self,hash_key): 
        '''
         Creates new directories,two new directories for every old directory.For the bucket where the overflow 
         occurs we empty the new buckets and and the local depth increases by one.The remaining buckets get the
         values of the previous keys and the value of the local depth remains the same. 
         
         Args:
          hash_key:str.The hashed key where the bucket overflow occurs.

        '''
        for key1 in list(self.data): 
          if(key1==hash_key): 
            self.data['0'+key1]=Bucket(bucket=[],ld=self.data[key1].ld+1)
            self.data['1'+key1]=Bucket(bucket=[],ld=self.data[key1].ld+1)
          else:
            self.data['0'+key1]=Bucket(bucket=self.data[key1].bucket,ld=self.data[key1].ld)
            self.data['1'+key1]=Bucket(bucket=self.data[key1].bucket,ld=self.data[key1].ld)

        for key2 in list(self.data): #delete previous keys
          if len(key2)!=self.global_depth:
            del self.data[key2]
         
    def rehashing(self,list1):
      '''
        Places the tuples in the correct bucket.

        Args:
         list1:list.Contains the tuples that need to be placed in the correct bucket.
      '''
      for key,value in(list1):
        hash_key = str(self.get_hash_index(key))
        if len(self.data[hash_key].bucket)<self.capacity and ((key,value)) not in (self.data[hash_key].bucket):
         self.data[hash_key].bucket.append((key,value))
    
    
    def find(self,operator,value):
      '''
        Returns a list that contains indexes which point to the rows of the table where the condition is true. 

        operator: string. The provided evaluation operator.
        value: int. The value being searched for.

      '''
      rows = []
      for key in self.data: # in each key
        for k,ind in self.data[key].bucket: # in each bucket
          if operator == '=':
            if k == value:
              rows.append(ind)
          elif operator == '>':
            if k > value:
              rows.append(ind)
          elif operator == '>=':
            if k >= value:
              rows.append(ind)
          elif operator == '<':
            if k < value:
              rows.append(ind)
          elif operator == '<=':
            if k <= value:
              rows.append(ind)

      #remove duplicates first
      rows = list(dict.fromkeys(rows))
      return rows

    def show(self):
        '''
        Print the whole dictionary (keys and values).
        '''
        for item in self.data:
         print("Key : {} , Value : {}".format(item,self.data[item].bucket))
              
class Bucket:
 '''
   The bucket abstraction.
 '''
 def __init__(self,bucket,ld):
   self.bucket= [] if bucket is None else bucket
   self.ld=ld
   
        
   
  