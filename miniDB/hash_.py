import hashlib

class Hash:
    def __init__(self, global_depth, max_bucket=4):
        '''
        The Hash abstraction.
        '''
        self.dict = {}
        self.buckets = []
        self.global_depth = global_depth
        self.bucket_max_size = max_bucket

        for i in range(pow(2, global_depth)):
            b = Bucket(global_depth)
            self.buckets.append(b)
            self.dict[i] = b

    def show(self):
        print("[", self.global_depth,"]")
        for i in range(pow(2, self.global_depth)):
            print("|| ", i, " || -> ", self.dict[i])

    def insert(self, value, ptr=-1):

        h = hash_func(value, pow(2, self.global_depth)) # to cell tou dictionary
        b = self.dict[h]  # apo to dictionary vriskw to swsto bucket
        b.data.append({"value": value, "ptr": ptr})   # vale proswrina to value sto bucket

        if len(b.data) <= self.bucket_max_size: # an exw xwro sto bucket apla kane append to value and meta return
            return

        elif len(b.data) > self.bucket_max_size: # den exw allo xwro sto bucket
            if b.local_depth == self.global_depth:  # an to local_depth tou bucket einai to idio me to global depth...
                self.global_depth += 1              # afxanoume to global depth 
                b.local_depth = self.global_depth # afxanoume to local depth 
                for i in range(pow(2, self.global_depth - 1), pow(2, self.global_depth)): # diplasiazoume to dictionary
                    self.dict[i] = self.dict[i - pow(2, self.global_depth-1)]

                nh = h + pow(2, self.global_depth-1) # to cell pou einai sxetiko me to conflict koitazei se neo bucket ( to nb )
                nb = Bucket(self.global_depth)
                self.buckets.append(nb)
                self.dict[nh] = nb

                b_data = b.data.copy()   # xwrise ta data anamesa sta duo buckets

                b.data = []

                for val in b_data:
                    hash_ = hash_func(val["value"], pow(2, self.global_depth))
                    self.dict[hash_].data.append(val)
            

                if len(b.data) >= self.bucket_max_size or len(nb.data) >= self.bucket_max_size:
                    try:
                        b.data.remove({"value": value, "ptr": ptr})
                    except:
                        pass
                    try:
                        nb.data.remove({"value": value, "ptr": ptr})
                    except:
                        pass

                    return self.insert(value, ptr)
            else: # an to local depth < global depth

                nb = Bucket(self.global_depth)
                self.buckets.append(nb)
                self.dict[h] = nb

                b.local_depth = self.global_depth

                b_data = b.data.copy()
                b.data = []

                for val in b_data:
                    hash_ = hash_func(val["value"], pow(2, self.global_depth))
                    self.dict[hash_].data.append(val)

                if len(b.data) >= self.bucket_max_size or len(nb.data) >= self.bucket_max_size:
                    try:
                        b.data.remove({"value": value, "ptr": ptr})
                    except:
                        pass
                    try:
                        nb.data.remove({"value": value, "ptr": ptr})
                    except:
                        pass
                    
                    return self.insert(value, ptr)
                
        else:
            raise("critical error: more data in bucket than accepted")

    def find(self, value):
        h = hash_func(value, pow(2, self.global_depth)) # to cell tou dictionary
        b = self.dict[h]  # apo to dictionary vriskw to swsto bucket

        for val in b.data:
            if value == val["value"]:
                return [val["ptr"]]
        return []

class Bucket:
    def __init__(self, local_depth):
        self.data = []
        self.local_depth = local_depth
    
    def __str__(self):
        return "[" + str(self.local_depth) + "] ... [" + ' '.join(str(e) for e in self.data) + "]" 


def hash_func(value, m):
    hash_object = hashlib.sha1(bytes(str(value), encoding='utf-8'))
    hex_dig = hash_object.hexdigest()
    res = int(hex_dig,32)
    return res % m