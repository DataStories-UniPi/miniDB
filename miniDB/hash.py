#array_len = 4
#arxikopoioume to megethos tou array
#values =[None] * array_len
#dimiourgoyme mia lista me ta stoixeia mas

#def hashing_function(key):
#    return hash(key) % len(array_len)

class Bucket(object): #object
    #length = 4
    values = []
    def __init__(self, length):
        #arxikopoioume ena keno array
        print("lenfgth")
        print(length)
        self.values =[" "]*length
        print("values_first")
        print(self.values)

    def hash(self, key):
        length = len(self.values)
        return hash(key) % length
    #h sinartisi mas dinei to index gia ena sigkekrimeno str key

    def add(self, key, value):
        #tha prosthesoyme sto array mas ena value apo to key tou value
        index = self.hash(key)
        print("values_second")
        print(self.values)
        print("index")
        print(index)
        if self.values[index] is not " ":
            for k1 in enumerate(self.values[index]):
                if k1[0] == key:
                    y = list(k1)
                    print(y)
                    y[0] = value
                    print(y)
                    k1 = tuple(y)
                    break
                else:
                    self.values[index].append([key, value])
        else:
            self.values[index] = []
            print("key")
            print(key)
            print("value")
            print(value)
            self.values[index].append([key, value])
            print("values_finish")
            print(self.values)

        if self.is_full():
            self.double_bucket()

    def get(self, key):
        index = self.hash(key)

        if self.values[index] is " ":
            print("This key: "+ key+" can not be found")
            raise KeyError()

        else:
            for k1 in enumerate(self.values[index]):
                print("1st")
                print(k1[0])
                print("2nd")
                print(key)
                if k1[0] == key:
                    return k1[1]

            print("This key: " + key + " can not be found")
            raise KeyError()

    def is_full(self):

        items = 0

        for item in self.values:
            if item is not None:
                items += 1
        return items + 1 == len(self.values)
    # boolean : true if items in values are one less than the length of the list

    def double_bucket(self):
        length = len(self.values * 2)
        h1 = Bucket(length)
        for i in range(len(self.values)):

            if self.values[i] is None:
                continue

        #h lista mas tora exei to diplasio megethos opote prepei na
        # prostethoun ksana oi times

            for k1 in self.values[i]:
                h1.add(k1[0], k1[1])

        self.values = h1.values

    def __getitem__(self, key):
        return self.get(key)