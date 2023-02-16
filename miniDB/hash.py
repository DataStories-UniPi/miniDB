class HashIndex:
    def __init__(self, table, column, name):
        self.index_name = name
        self.index = {}
        self.table = table
        self.column = column
     #The constructor then builds a hash index by iterating through each row of the table and adding each row to a list associated with the appropriate key in the index.
        for row in self.table:
            key = row[self.column]
            if key in self.index:
                self.index[key].append(row)
            else:
                self.index[key] = [row]
    #lookus takes a key and returns the list of rows associated with that key
    def lookup(self, key):
        return self.index.get(key, [])
