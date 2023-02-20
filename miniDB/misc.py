import operator

def get_op(op, a, b):
    '''
    Get op as a function of a and b by using a symbol
    '''
    ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq}

    try:
        return ops[op](a,b)
    except TypeError:  # if a or b is None (deleted record), python3 raises typerror
        return False

def split_condition(condition):
    ops = {'>=': operator.ge,
           '<=': operator.le,
           '=': operator.eq,
           '>': operator.gt,
           '<': operator.lt}

    for op_key in ops.keys():
        splt=condition.split(op_key)
        if len(splt)>1:
            left, right = splt[0].strip(), splt[1].strip()

            if right[0] == '"' == right[-1]: # If the value has leading and trailing quotes, remove them.
                right = right.strip('"')
            elif ' ' in right: # If it has whitespaces but no leading and trailing double quotes, throw.
                raise ValueError(f'Invalid condition: {condition}\nValue must be enclosed in double quotation marks to include whitespaces.')

            if right.find('"') != -1: # If there are any double quotes in the value, throw. (Notice we've already removed the leading and trailing ones)
                raise ValueError(f'Invalid condition: {condition}\nDouble quotation marks are not allowed inside values.')

            return left, op_key, right

def reverse_op(op):
    '''
    Reverse the operator given
    '''
    return {
        '>' : '<',
        '>=' : '<=',
        '<' : '>',
        '<=' : '>=',
        '=' : '='
    }.get(op)


    def init(self, column_name, operator, value):
        self.column_name = column_name
        self.operator = operator
        self.value = value

    def invert(self):
        return Where(self.column_name, "NOT " + self.operator, self.value)

        def init2(self, column_name, operator, value):
            self.column_name = column_name
            self.operator = operator
            self.value = value

        def between(self, value1, value2):
            return Where(self.column_name, "BETWEEN", (value1, value2))

        def str(self):
            if self.operator == "BETWEEN":
                return f"{self.column_name} BETWEEN {self.value[0]} AND {self.value[1]}"
            else:
                return f"{self.column_name} {self.operator} {self.value}"


        def where_clause(self, where):
            if not where:
                return ""
            elif isinstance(where, Where):
                return f"WHERE {where}"
            else:
                conditions = " AND ".join(str(w) for w in where)
                return f"WHERE {conditions}"

    
        def where_clause(self, where):
            if not where:
                return ""
            elif isinstance(where,





