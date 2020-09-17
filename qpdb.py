from tabulate import tabulate
import pickle

# io for the database, joins, query opt, server-client (ports), user priviledges, sort rows

import operator

def get_op(op, a, b):
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '==': operator.eq}
    try:
        return ops[op](a,b)
    except:
        raise Exception(f'Unknown operator "{op}"')


class Database:
    def __init__(self):
        self.tables = {}
        # self.no_of_tables = len(self.tables)
        self.len = 0

    def _update(self):
        self.len = len(self.tables)

    def create_table(self, name=None, column_names=None, column_types=None, load=None):
        self.tables.update({name: Table(name=name, column_names=column_names, column_types=column_types, load=load)})
        # self.name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        if name not in self.__dir__():
            setattr(self, name, self.tables[name])
        else:
            raise Exception(f'"{name}" attribute already exists in "{self.__class__.__name__} "class.')
        # self.no_of_tables += 1
        self._update()

    def drop_table(self, name):
        del self.tables[name]
        delattr(self, name)

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
                self.__dict__.update(load)
            elif isinstance(load, str):
                self._load_from_file(load)

        elif (name is not None) and (column_names is not None) and (column_types is not None):

            self.name = name

            if len(column_names)!=len(column_types):
                raise ValueError('Need same number of column names and types.')

            self.column_names = column_names

            self.columns = []

            for col in self.column_names:
                if col not in self.__dir__():
                    setattr(self, col, [])
                    self.columns.append([])
                else:
                    raise Exception(f'"{col}" attribute already exists in "{self.__class__.__name__} "class.')

            self.column_types = column_types
            self._no_of_columns = len(column_names)
            self.data = []
            # self.columns = [[] for _ in range(self._no_of_columns)]
            self._update()



        else:
            print("Created table is an empty object. Are you sure you know what you're doing?")

    def _update(self):
        self.columns = [[row[i] for row in self.data] for i in range(self._no_of_columns)]
        for ind, col in enumerate(self.column_names):
            setattr(self, col, self.columns[ind])

    def insert(self, row):
        # row = row.split(',')

        if len(row)!=self._no_of_columns:
            raise ValueError(f'ERROR -> Cannot insert {len(row)} values. Only {self._no_of_columns} columns exist')

        for i in range(len(row)):
            try:
                row[i] = self.column_types[i](row[i])
            except:
                raise ValueError(f'ERROR -> Value {row[i]} is not of type {self.column_types[i]}.')
                return
        self.data.append(row)
        self._update()

    def delete(self, row_no):
        self.data.pop(row_no)

    def _select_indx(self, rows):
        if not isinstance(rows, list):
            rows = [rows]
        dict = {(key):([self.data[i] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        return Table(load=dict)

    def select_where(self, column_name, operator, value):
        column = self.columns[self.column_names.index(column_name)]
        rows = [ind for ind, x in enumerate(column) if get_op(operator, x, value)]

        dict = {(key):([self.data[i] for i in rows] if key=="data" else value) for key,value in self.__dict__.items()}
        return Table(load=dict)


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
        tmp_dict = pickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict)

    def where(self, column_name, op, value):
    # where syntax -> db.table.where(colname, <, 10)
    # this works -> db.test1.select(db.test1.where('Age','<=',40)).show()
        column = self.columns[self.column_names.index(column_name)]
        idxs = [ind for ind, x in enumerate(column) if get_op(op, x, value)]
        return idxs
