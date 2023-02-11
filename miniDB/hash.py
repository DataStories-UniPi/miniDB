import math
class Hash:
    def __init__(self):
     self.size = 2
     self.data=[[] for i in range(self.size)]
     #print('Self data is:',self.data)
    
    def get_hash_index(self,key):
      if type(key)==int:
       h=key
      elif type(key)==float:
        h=math.ceil(key)
      else:
         h=0
         for c in key:
           h+=ord(c)
      print("key is: ",key)
      print("h is: ",h)
      print("len of self data is: ",len(self.data))
      hash=h%len(self.data) #2,4,8
      print("hash value is: ",h,"%",len(self.data),"=",hash)
      hash_index=int(bin(hash)[2:])
      #LSB()
      print('hash_index: ',hash_index)
      #print("\n")
      return hash_index
    
    def LSB(num, K):
     return bool(num & (1 << (K - 1) ))
    
    def insert(self,key,value):
      
      hash_key=self.get_hash_index(key)
      bucket=self.data[hash_key]
      
      found_key = False
      for index, record in enumerate(bucket):
        record_key, record_val = record
        if record_key == key and record_val == value:
          found_key = True
          break
      print("bucket's length is: ",len(bucket))
      if found_key:
        print("key found")
        bucket[index] = (key, value)
      else:
        print("key not found")
        if len(bucket) == self.size:
          print("full backet!")
        bucket.append((key, value))
      
      print(self.data)