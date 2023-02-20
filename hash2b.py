class Bucket:
    def __init__(self, Depth):
        self.Local_depth = Depth
        self.Bucket_list = []

    def insert(self,value):
         if self.Local_depth == len(self.Bucket_list): 
            return False 
        else:
            self.Bucket_list.append(value)
            return True

    def find(self, value):
         for i in self.Bucket_list: 
            if i[0] == value:
                return i[1]
        
        return False 
        
    def remove(self, value):
        for i in self.Bucket_list: 
            if i[0] == value:
                self.Bucket_list.remove(i)
                return True
        
        return False 

    def empty(self):
         tempList = []  
        for i in self.Bucket_list:
            tempList.append(i) 
            self.Bucket_list.remove(i)

        return tempList 

    def update_depth(self, newdepth):
        self.Local_depth = newdepth

    def show(self):
        print(self.Bucket_list)
        
 class Hash:
     def __init__(self, Number_of_elements=1):

        self.Global_depth = 1 

        while (2**self.Global_depth)*self.Global_depth < Number_of_elements:
            self.Global_depth += 1                     
               
        self.Buckets = {}

        for i in range(2**self.Global_depth): 
            self.Buckets[bin(i)] = Bucket(self.Global_depth)

    def insert(self, value, index):
         bucket_to_insert = bin(value % (2**self.Global_depth))

        insertion = self.Buckets[bucket_to_insert].insert([value,index])

        while not insertion: 
            self.directory_expansion()
            
            insertion = self.insert(value,index)

        return True
 
    def find(self, value):
         bucket_to_look_for = bin(value % (2**self.Global_depth)) 
        return self.Buckets[bucket_to_look_for].find(value) 

    def remove(self, value):
        
        bucket_to_look_for = bin(value % (2**self.Global_depth))
        return self.Buckets[bucket_to_look_for].remove(value)

    def directory_expansion(self):
         current_buckets = 2**self.Global_depth
        self.Global_depth += 1 
        buckets_needed = 2**self.Global_depth

      
        for i in range(current_buckets):
            self.Buckets[bin(i)].update_depth(self.Global_depth)

        for i in range(current_buckets, buckets_needed):
            self.Buckets[bin(i)] = Bucket(self.Global_depth)

        for i in range(current_buckets):
            tempList = self.Buckets[bin(i)].empty() 
            for j in tempList:
                self.insert(j[0], j[1])
            
    def show(self):
         print('\nBuckets: \n')
        for i in range(2**self.Global_depth):
            print (format(i,'b').zfill(self.Global_depth) , end =" ")
            self.Buckets[bin(i)].show()