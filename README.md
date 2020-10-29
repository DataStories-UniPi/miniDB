# miniDB

The miniDB project is a minimal and easy to expand and develop for RMDBS tool, written excusivelly in python3. MiniDB's main goal is to provide the user with as much functionality as posssible while being easy to understand and even easier to expand. Thus, miniDB's primary market are students and researchers that want to work with a tool that they can understand through and through while being able to add a new feature as quickly as possible.

## Installation

Install the only dependency with the following command:
``` Python
pip install tabulate
```

## Documentation

TBA

## Loading the [smallRelations database](https://www.db-book.com/db6/lab-dir/sample_tables-dir/index.html)

To create a database containing the smallRelations tables and get an interactive shell, run 
``` Python
python -i smallRelationsInsertFile.py
```
You can the access the database through the db object that will be available. For example, you can show the contents of the student table by running the following command:
``` Python
>> db.show_table('student')
```
The database wil be save with the name "smdb". You can load the database in another python shell by running the following commands:
``` Python
>> from database import Database
>> db = Database("smdb", load=True)
```

## Contributors
George S. Theodoropoulos, Yannis Theodoridis; Data Science Lab., University of Piraeus.
