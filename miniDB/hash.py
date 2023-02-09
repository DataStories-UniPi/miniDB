
def hashh(h):
    '''Hashes the values'''

    # choosing mod 1024 since i think that 1023 unique values are enough for this assignment
    return bin(hash(h)*31%1024).removeprefix('0b')
class Directory:
    def __init__(self):
        '''Starts with global depth of 1, 2 buckets, and a list of buckets (in order to help operations)'''

        self.global_depth=1
        self.dic={'0': Bucket('0', 1), '1' : Bucket('1', 1) }
        self.lst_of_buckets=[self.dic['0'], self.dic['1']]

    def bucket_split(self, bucket, value):
        '''Create 2 new buckets from the split and redistribute the keys of the bracket'''

        if bucket in self.lst_of_buckets:
            self.lst_of_buckets.remove(bucket)
        new_bck1 = Bucket('0' + bucket.lsb, bucket.local_depth + 1)
        new_bck2 = Bucket('1' + bucket.lsb, bucket.local_depth + 1)
        self.lst_of_buckets.extend([new_bck1, new_bck2])
        if bucket.lsb in self.dic.keys():
            self.dic.pop(bucket.lsb)
        for element in bucket.lst:
            if element[-new_bck1.local_depth:] == new_bck1.lsb:
                new_bck1.lst.append(element)
                new_bck1.capacity += 1
            else:
                new_bck2.lst.append(element)
                new_bck2.capacity += 1

        if value[1][-bucket.local_depth-1:]:
            new_bck1.lst.append(value)
            new_bck1.capacity += 1
        else:
            new_bck2.lst.append(value)
            new_bck2.capacity += 1

        for key in self.dic.keys():
            if self.dic[key] ==  bucket:
                if key.endswith(new_bck1.lsb):
                    self.dic[key] = new_bck1
                else:
                    self.dic[key] = new_bck2

    def directory_expansion(self):
        '''Doubling the size of directory and increasing global depth'''

        self.global_depth += 1
        for key in self.dic.copy().keys():
            self.dic['0' + key] = Bucket('0' + key, len('0' + key))
            self.dic['1' + key] = Bucket('1' + key, len('1' + key))

    def rehashing(self):
        '''Map the buckets according to the new directory'''

        # removing the keys whose len is less than the global depth
        for key in self.dic.copy().keys():
            if len(key) < self.global_depth:
                self.dic.pop(key)

        # putting in a tmp list the keys that are matched perfectly with a bucket, so they wont be in the selection after
        tmp_key_lst = []
        for bucket in self.lst_of_buckets:
            if bucket.local_depth == self.global_depth:
                for key in self.dic.keys():
                    if key == bucket.lsb:
                        self.dic[key] = bucket
                        tmp_key_lst.append(key)
                        continue
        for key in self.dic.keys():
            if key not in tmp_key_lst:
                for bucket in self.lst_of_buckets:
                    if key.endswith(bucket.lsb):
                        self.dic[key] = bucket
                        continue


    def insert(self, value):
        '''Insert the value in a bucket'''

        for key in self.dic.copy().keys():
            if value[1][-self.global_depth:] == key:
                if self.dic[key].capacity < 3:
                    self.dic[key].capacity += 1
                    self.dic[key].lst.append(value)
                    break
                elif self.global_depth > self.dic[key].local_depth:
                     self.bucket_split(self.dic[key], value)
                     break
                else:
                    self.directory_expansion()
                    self.bucket_split(self.dic[key], value)
                    self.rehashing()
                    break

    def build(self, val):
        '''Building by first hashing the value and then passing it to the insert function'''

        value = (val[0], hashh(val[1]).zfill(10))
        self.insert(value)

    def find(self, value):
        for key in self.dic.keys():
            if key == hashh(value).zfill(10)[-len(key):]:
                print("to vrik")
                for i, j in self.dic[key].lst:
                    print(i,j)
                    print(hashh(value).zfill(10))
                    if hashh(value).zfill(10) == j:
                        return i
class Bucket:
    def __init__(self, lsb, local_depth):
        '''Local depth, LSB, list of values and capacity'''

        self.local_depth=local_depth
        self.lsb = lsb
        self.lst = []
        self.capacity = 0

