from tabulate import tabulate
class Table:
    '''
    Table object represents a table inside a database
    
    A Table object can be created either by assigning:
        - a table name (string)
        - column names (list of strings)
        - column types (list of functions like str/int etc)
    
    OR
        
        - by assigning a value to the variable called load. This value can be:
            - a path to a Table file saved using the save function
            - a dictionary that includes the appropriate info (all the attributes in __init__)
            
    '''
    def __init__(self, name=None, column_names=None, column_types=None, load=None):
        
        if load is not None:
            if isinstance(load, dict):
                self.__dict__.update(_dict)
            elif isinstance(load, str):
                self._load_from_file(load)
            
        elif (name is not None) and (column_names is not None) and (column_types is not None):
            
            self.name = name

            if len(column_names)!=len(column_types):
                raise ValueError('Need same number of column names and types.')

            self.column_names = column_names
            self.column_types = column_types
            self._no_of_columns = len(column_names)
            self.data = []
        else:
            raise Warning("Created table is an empty object. Are you sure you know what you're doing?")

    def insert(self, row):
        row = row.split(',')
        
        if len(row)!=self._no_of_columns:
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {self._no_of_columns} columns exist')
            
        for i in range(len(row)):
            try:
                row[i] = self.column_types[i](row[i])
            except:
                raise ValueError(f'ERROR -> Value {row[i]} is not of type {self.column_types[i]}.')
                return
        self.data.append(row)

    def delete(self, row_no):
        self.data.pop(row_no)

    def select(self, rows):
        if not isinstance(rows, list):
            rows = [rows]
        return {(key):([self.data[i] for i in rows] if key=="data" else value) for key,value in tab.__dict__.items()}


    def show(self, no_of_rows=5):
        print(f"# {self.name} #\n")
        print(tabulate(self.data[:no_of_rows], headers=self.column_names))

    def save(self, filename):
        if filename.split('.')[-1] != 'pkl':
            raise ValueError(f'ERROR -> Savefile needs .pkl extention')
            
        with open(filename, 'wb') as f:
            pickle.dump(self.__dict__, f)

    def _load_from_file(self, filename):
        f = open(filename, 'rb')
        tmp_dict = cPickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict)
        
        
