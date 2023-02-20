# import hashlib

# class HashTable(object):
#     def __init__(self, size=10)
     
#      self.num_elements = 0
#      self.data = [0] * size
#      self.size = len(self.data)
#      print(self.data)

#     def __get_hash_index(self):
#      test_hash = int(blah blah)
#      return test_hash % self.size 

#     # making insert method   
#     def insert(self, key, value):
#      '''
#     inserting data
#     key(str) , value(tuple) ,
#     '''
#     hash_data = (key, value)
#     hash_index = self.__get_hash__index(key)
#     self.data[hash_index] = hash_data
  

#     # getter
#     def get(self,key):
#     '''data from key'''
#      hash_index = self.__get__hash_index(key)
#      #compare with first element of the list
#     if key != self.data[hash_index][0] or self.data[hash_index] ==0
#      datakey[0]
#     if key !=datakey or data ==0:
#        raise KEYeRROR("cant hash that key or no data")
#     return data[1]
  
#     #similar for remove 
#     def remove(self, key):
#      hash_index = self.__get__hash_index(key)
#      #compare with first element of the list
#      if key != self.data[hash_index][0] or self.data[hash_index] ==0
#      datakey[0]
#      if key !=datakey or data ==0:
#        raise KEYeRROR("cant hash that key or no data")
#      self.data[hash_index] = 0


#     def key_contains(self, substring):


#     test_hash_table = HashTable()