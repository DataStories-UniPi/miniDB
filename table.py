class Table:
    def __init__(self):
        self.data = []
        self.colnames = []
        self.coltypes = []
        self._update()

    def _update(self):
        self.no_cols = len(self.colnames)
        self.columns = [[row[i] for row in self.data]for i in range(self.no_cols)]
        self.size = len(self.data)

    def set_column_names(self, colnames):
        self.colnames = colnames

    def set_column_types(self, coltypes):
        self.coltypes = coltypes

    def insert(self, row):
        row = row.split(',')
        if self.no_cols != 0:
            # if insert without col info add all data as text with no column names
            for i in range(len(row)):
                try:
                    row[i] = self.coltypes[i](row[i])
                except:
                    print('nope')
                    return
        else:
            self.colnames = list(range(len(row)))
            self.coltypes = [str for _ in range(len(row))]
        self.data.append(row)
        self._update()

    def delete(self, row_no):
        self.data.pop(row_no)
        self._update()

    def select(self, rows):
        if not isinstance(rows, list):
            rows = [rows]
        print(tabulate([tab.data[i] for i in rows], headers=self.colnames))

    def show(self):
        print(tabulate(self.data, headers=self.colnames))

    def save(self, filename):
        with open(f'{filename}.pkl', 'wb') as f:
            pickle.dump(self.__dict__, f)

    def load(self, filename):
        f = open(filename, 'rb')
        tmp_dict = cPickle.load(f)
        f.close()

        self.__dict__.update(tmp_dict)
        
