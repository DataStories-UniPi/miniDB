import math
class Hash:
    def __init__(self):
     #self.size=2
     self.capacity=3
     self.global_depth=1
     bucket1=Bucket(bucket=[],ld=1,key='0')
     bucket2=Bucket(bucket=[],ld=1,key='1')
     self.data={'0':bucket1,'1':bucket2}
     #self.local_depth={'0':1,'1':1}
     #self.prefix=[self.data[0],self.data[1]]
     #self.number_of_lsb=1
     #print(self.data)

    def get_hash_index(self,key):
      print('Key is: ',key)
      if type(key)==int:
        h=key
      elif type(key)==float:
        h=math.ceil(key)
      else: # type string
        h=0
        for c in key:
          h+=ord(c) # ascii number
      size=2**(self.global_depth)
      hash=h%(size) #2,4,8
      hashed=int(bin(hash)[2:])
      hash_index=str(hashed)
      if(self.global_depth>1):
       if(len(hash_index)!=self.global_depth):
         hash_index=hash_index.zfill(self.global_depth)
       
    
      #print("HASHED INDEX:",hash_index)
      return hash_index
   
    def insert(self,key,value):
      hash_key = str(self.get_hash_index(key))
      found_key = False

      for record in enumerate(self.data):
        record_key, record_val = record
        #print("record key is: ",record_key)
        #print("record_value is: ",record_val)
        
        if record_key == key and record_val==value:
          found_key = True
          break     
      #if found_key:
        #print("if here")
        #self.bucket[index] = (key, value)
      
      if not found_key:
        print("HASH KEY:",hash_key)
        #print("Bucket's Length is: ",len(self.data[hash_key].bucket))
        print("Bucket is: ",self.data[hash_key].bucket)
        
        if len(self.data[hash_key].bucket)==self.capacity:
          print("Bucket is full!")
          print("Buckets must be splitted!")
          print("Record key that caused split is:",key)
           
          self.split_bucket(key,hash_key,value)  # call split function 
        
        else: # no split -> add tuple to bucket
          print("Value is not in the dictionary -- ADD NEW VALUE!")
          self.data[hash_key].bucket.append((key,value))
 
      '''
      for item in self.data:
       print("Key : {} , Value : {}".format(item,self.data[item].bucket))
      '''
        
    def split_bucket(self,key,hash_key,value):
     list1=[]  
     for key1,value1 in(self.data[hash_key].bucket):   
         #print("keY VALUE:",(key1,value1))
         list1.append((key1,value1))

     if((key,value)) not in list1:
       list1.append((key,value))
     
     if self.global_depth==self.data[hash_key].ld:   # global depth = local depth    
        #print("List is",list1)
        self.global_depth+=1
        #print("global depth is:",self.global_depth)
        self.directory_expansion(hash_key)
        self.rehashing(list1)

     elif self.data[hash_key].ld<self.global_depth:
        for key in (self.data):
          if self.data[hash_key].bucket==self.data[key].bucket and len(key)==len(hash_key) and key!=hash_key: # keys point to the same bucket
             #print(self.data[hash_key].bucket)
             #print(self.data[key].bucket)
             self.data[key].bucket=[]
             self.data[key].ld+=1
             #print('YASS KEY IS:',key)
             #print("LDDD IS",self.data[key].ld)
             break
        #print("hashed keyyyyy:",hash_key)
        self.data[hash_key].bucket=[]
        self.data[hash_key].ld+=1
        #print(" 2nd LDDD IS",self.data[hash_key].ld)
         #self.data[key].bucket = []
        self.rehashing(list1)
     
    def directory_expansion(self,hash_key): 
        for key1 in list(self.data): 
          if(key1==hash_key):
            self.data['0'+key1]=Bucket(bucket=[],ld=self.data[key1].ld+1,key='0'+key1)
            self.data['1'+key1]=Bucket(bucket=[],ld=self.data[key1].ld+1,key='1'+key1)
          else:
            self.data['0'+key1]=Bucket(bucket=self.data[key1].bucket,ld=self.data[key1].ld,key='0'+key1)
            self.data['1'+key1]=Bucket(bucket=self.data[key1].bucket,ld=self.data[key1].ld,key='1'+key1)

        for key2 in list(self.data):
          if len(key2)!=self.global_depth:
            del self.data[key2]

        #print(self.data.keys())

        '''
        for item in self.data:
         print("Key : {} , Value : {}".format(item,self.data[item].bucket))
         print("LD ISS:",self.data[item].ld)
        '''
            
    def rehashing(self,list1):
      #print('LISSSSST1 IS:',list1)
      for key,value in(list1):
        hash_key = str(self.get_hash_index(key))
        #print("HASSSH KEY:",hash_key)
        self.data[hash_key].bucket.append((key,value))


    def show(self):
        '''
        Print the bucket's value and relevant information.
        '''
        print("Show function here!")
        print(self.data)
        for item in self.data:
         print("Key : {} , Value : {}".format(item,self.data[item].bucket))
         #print("LD ISS:",self.data[item].ld)
        
        '''
        for key in self.data:
          self.data[key].bucket.show()
        '''

       

'''
  def directory_expansion(self):
      
      self.size=2*self.size
      print("HASHHH:",hash_key)
      h1=hash_key
      k1='0'+h1
      h2='1'+h1
      
      self.global_depth+=1
'''

class Bucket:
 
 def __init__(self,bucket,ld,key):
   self.bucket= [] if bucket is None else bucket
   self.ld=ld
   self.key=key
   
 def show(self):
        '''
        Print the buckets's tuples.
        '''
        for item in self.bucket:
          print(item)
        #print('Values', self.bucket)
        
   
  