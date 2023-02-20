import string

letters = string.ascii_lowercase

def between(value,range):
    # checks if value is between range
    # value: the specific value stored in table we are comparing
    # range: range of accepted values from between keyword; is string; must contain split_condition
    split_condition='&' # exp: BETWEEN 5 & 25;
    if(split_condition not in range):
        raise IndexError('Between syntax: BETWEEN "value1 & value2"')    
    try: # comparing floats-ints
        range = [float(x) for x in range.split('&')] # splits the between range
        float(value) # will work if value we are comparing is float or int
    except ValueError: # are we comparing strings?
        range = range.split('&') # range input must not include the split character
    if ((value>=range[0] and value<=range[1]) or (value>=range[1] and value<=range[0])): # BETWEEN 5 & 10 == BETWEEN 10 & 5
        print(value)
        return True
    else: 
        return False

# for i in letters:
#     print(i,between(i,"g&d"))

# print ("!!!")
# if("b"<="bamboo" and "b">="lama"):
#     print("it is")
flag = False
range1="5&10"
print("Printing True values")
for i in range (100):
    if(not flag):
        print("range:",range1)
        flag = True
    between(i,range1)
# for i in reversed(range(100)):
#    print(i,between(i,"25&50"))