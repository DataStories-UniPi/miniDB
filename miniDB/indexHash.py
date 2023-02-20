import math


class Bucket:
    def __init__(self, values, local_depth):
        self.values = values
        self.local_depth = local_depth


'''
INFO:
    An element is inserted into a bucket, if that bucket overflows then:
     case 1: The bucket's local depth is equal to global depth
        - directory expansion is performed
        - bucket split is performed (each new bucket will have its local depth increased by 1)
    case 2: The bucket's local depth is less than the global depth
        - bucket split is performed (each new bucket will have its local depth increased by 1)
        
'''


def get_binary(value):
    # initializing string
    test_str = str(value)

    # printing original string
    # print("The original string is : " + str(test_str))

    # using join() + ord() + format()
    # Converting String to binary
    res = ''.join(format(ord(i), '08b') for i in test_str)

    # printing result
    # print("The string after binary conversion : " + str(res))
    return res


def insert(dict, position, elem, global_depth):
    new_dict = {}
    print('dict', 'position ', position, 'element ', elem)
    number_of_bucket_elements = len(dict[position].values)
    print(number_of_bucket_elements)
    print('local depth', dict[position].local_depth)
    # There is no overflow
    if (number_of_bucket_elements < 3):
        dict[position].values += [elem]

    # There is overflow
    else:
        # Case 1 local depth equal to global depth -> directory expansion + bucket split(+ local depth will increase
        # by 1)
        if dict[position].local_depth == global_depth:
            print('case 1')
            # dictionary expansion
            global_depth = int(math.sqrt(len(dict))) +1
            print('global d',global_depth)
            # create new directory of size 2 ^ global depth
            for i in range(pow(2, global_depth)):
                print("i ", i, "pow(2,global_depth+1) ", pow(2, global_depth + 1), "format(i,'b') ", format(i, 'b'))
                new_dict.setdefault(format(i, 'b'), Bucket([], 1))
                # print('new dict ',new_dict[format(i,'b')].values)
            print(new_dict.keys())
            # rehash the new directory
            for i in range(global_depth + 1):
                # if this bucket is from the old dictionary and is not the one to be split it should be saved as it is
                if i < pow(2, global_depth - 1) and i != int(position):
                    new_dict[format(i, 'b')] = dict[format(i, 'b')]
                    # reduce global depth by one so ONLY the split buckets will get an increased global depth
                    new_dict[format(i, 'b')].local_depth -= 1
                # the bucket being split

            #get the elements fromthe overflowed bucket + the new element
            elements = dict[position].values
            elements += [elem]
            # position the elements according to the hash function
            for item in elements:
                print('elements are ', elements)
                # Convert to binary
                b = get_binary(item)
                # Hash function => result mod 2 ^ global depth
                lsb = int(b) % pow(2, global_depth)
                insert(new_dict, format(lsb, 'b'), item, global_depth)

            # update the old dictionary
            for count, value in enumerate(new_dict):
                print('Nkey', value, 'Bucket ', new_dict[value].values, ' local depth ', new_dict[value].local_depth)
                # first half the already exists
                if count < pow(2, global_depth - 1):
                    dict[value].values = new_dict[value].values
                # creating and updating second half that did not exist before
                else:
                    dict.setdefault(value, Bucket([], 1))
                    dict[value].values = new_dict[value].values
                # increment local depths affected by the expansion by 1
                if len(dict[value].values) >0:
                    dict[value].local_depth = new_dict[value].local_depth + 1
        # local depth < global depth, in that case only a bucket split will be preformed
        else:
            print('case 2')
            # get bucket elements + new element
            elements = dict[position].values
            elements += [elem]

            #get current local depth of the bucket being split
            local_d= dict[position].local_depth

            # empty this bucket
            dict[position].values =[]


            # position the elements according to the hash function
            for item in elements:
                print('elements are ', elements)
                # Convert to binary
                b = get_binary(item)
                # Hash function => result mod 2 ^ global depth
                lsb = int(b) % pow(2, global_depth)
                # lsb points to the bucket in which the elements will be stored
                # we want to increase this bucket's local depth by 1
                dict[format(lsb, 'b')].local_depth +=1
                insert(dict, format(lsb, 'b'), item, global_depth)

def get_hash_index(list_of_values):
    global_depth = 1
    buckets = [Bucket([], 1), Bucket([], 1)]
    dictionary = {}
    dictionary.setdefault('0', buckets[0])
    dictionary.setdefault('1', buckets[1])
    for element in list_of_values:
        # Convert to binary
        b = get_binary(element)
        # Hash function => result mod 2 ^ global depth
        lsb = int(b) % pow(2, global_depth)
        # print(b," ", pow(2,global_depth)," ",lsb)
        # Insert element in bucket and dictionary
        insert(dictionary, str(lsb), element, global_depth)
    for x in dictionary:
        print('key', x, 'Bucket ', dictionary[x].values, ' local depth ', dictionary[x].local_depth)


lista = [1, 2, 3, 4, 5, 6, 7,8]
get_hash_index(lista)
