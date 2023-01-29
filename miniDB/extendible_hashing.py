from page import Page
class ExtendibleHashing:
    '''
    Class that is used for the Hash index functionality.
    '''
    def __init__(self) -> None:
        self.global_depth = 0 # Set global depth (d) equal to zero (d=0)
        self.directory = [Page()] # The directory is a list with one Page object (bucket)

    def get_page(self, key):
        h = hash(key) # Compute the hash values of the key
        return self.directory[h % (1 << self.global_depth)] # Modulo operation: hash % (2**d)

    def put(self, key, value) -> None:
        page = self.get_page(key) # Find the page/bucket that the value should be put
        full = page.full() # Find if the page/bucket is already full
        page.put(key, value) # Put the value to the appropriate page/bucket
        if full: # If the page/bucket was full
            if page.local_depth == self.global_depth: # If local depth of the page/bucket equals the global depth of the directory
                self.directory *= 2 # Make the length of the directory double
                self.global_depth += 1 # Add 1 to the global depth

            # The page/bucket must be split due to being full
            # Create new objects of Page class
            p0 = Page() 
            p1 = Page()
            p0.local_depth = p1.local_depth = page.local_depth + 1 # Increment the local depth for the new pages/buckets by 1
            high_bit = page.get_local_high_bit() # Find the highest bit of local depth
            for k2, v2 in page.map:
                h = hash(k2)
                
                if (h % (high_bit + 1)): # Find the modulo
                    new_p = p1 # Page p1 will contain the new value
                else:
                    new_p = p0 # Page p0 will contain the new value
                new_p.put(k2, v2) # Put the value to one of the two new pages/buckets

            # Add the pages/buckets to the directory list
            for i in range(hash(key) % high_bit, len(self.directory), high_bit):
                if (i % (high_bit + 1)):
                    self.directory[i] = p1
                else:
                    self.directory[i] = p0

    def get(self, key): # Returns the value for the specified key from the found bucket
        return self.get_page(key).get(key)

    
        
    