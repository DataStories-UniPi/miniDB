import pandas as pd

class RelationalAlgebra:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, columns, data):
        self.tables[name] = pd.DataFrame(data, columns=columns)
        print(f"Created table {name} with columns {columns} and data {data}")

    def select(self, table_name, condition):
        table = self.tables[table_name]
        selected_rows = table.query(condition)
        print(f"Selected rows from table {table_name} where {condition}:\n{selected_rows}")
        return selected_rows

    def project(self, table_name, columns):
        table = self.tables[table_name]
        projected_data = table[columns]
        print(f"Projected columns {columns} from table {table_name}:\n{projected_data}")
        return projected_data

    def join(self, table1_name, table2_name, join_condition):
        table1 = self.tables[table1_name]
        table2 = self.tables[table2_name]
        joined_data = pd.merge(table1, table2, how='inner', left_on=join_condition[0], right_on=join_condition[1])
        print(f"Joined data from tables {table1_name} and {table2_name} on {join_condition}:\n{joined_data}")
        return joined_data

# Example usage
db = RelationalAlgebra()

db.create_table("Students", ["ID", "Name", "Age"], [[1, "Alice", 20], [2, "Bob", 22]])
db.create_table("Courses", ["ID", "Course"], [[1, "Math"], [2, "History"]])

selected_students = db.select("Students", "Age > 20")
projected_students = db.project("Students", ["Name", "Age"])
joined_data = db.join("Students", "Courses", ["ID", "ID"])
