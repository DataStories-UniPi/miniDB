PAGE_SZ = 10
class Page: 
    '''
    Class that represents a page/bucket
    '''
    def __init__(self) -> None:
        self.map = [] # list that contains the values of the bucket
        self.local_depth = 0 # the starting local depth is 0 (l=0)

    def full(self) -> bool: # Return true if the bucket is full
        return len(self.map) >= PAGE_SZ

    def put(self, k, v) -> None: # Function that adds a value to the bucket
        for i, (key, value) in enumerate(self.map):
            if key == k: # If the key already exists
                del self.map[i] # Delete the previous value
                break
        self.map.append((k, v)) # Add the value to the bucket

    def get(self, k): # Returns  the value that corresponds to the given key
        for key, value in self.map:
            if key == k:
                return value

    def get_local_high_bit(self): # Find the highest bit of local depth
      return 1 << self.local_depth
