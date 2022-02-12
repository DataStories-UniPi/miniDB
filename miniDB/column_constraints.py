class ColumnConstraints:
    '''
    Used to store a list of every column names that has a constraint
    '''

    def __init__(self):
        self.not_null = list()
        self.unique = list()

    def _add_constraint(self, constraint, col_name):
        '''
        Saves the given column name in the list
        of the given constraint

        Args:
            constraint: A string of the constraint type
            col_name: The column name to save
        '''

        if constraint == "not null":
            self.not_null.append(col_name)
        elif constraint == "unique":
            self.unique.append(col_name)
    
    def _generate_dictionary(self):
        '''
        Generates a dictionary containing the constraint names
        as keys and the lists of column names as values
        '''
        dic = dict()
        dic["not_null"] = self.not_null
        dic["unique"] = self.unique
        print(dic)
        return dic

    def __str__(self):
        return f"not null : {self.not_null}, unique : {self.unique}"

    def __repr__(self):
        return f"not null : {self.not_null}, unique : {self.unique}"
