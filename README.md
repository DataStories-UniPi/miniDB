# miniDB

The miniDB project is a minimal and easy to expand and develop for RMDBS tool, written excusivelly in Python 3. MiniDB's main goal is to provide the user with as much functionality as posssible while being easy to understand and even easier to expand. Thus, miniDB's primary market are students and researchers that want to work with a tool that they can understand through and through, while being able to implement additional features as quickly as possible.

## Installation

Python 3.7 or newer is needed. To download and build the project run:

### - Lab Specific - 
```bash
git clone --single-branch --branch lab https://github.com/DataStories-UniPi/miniDB.git
cd miniDB
pip install -r requirements.txt
```

### - Latest - 
```bash
git clone https://github.com/DataStories-UniPi/miniDB.git
cd miniDB
pip install -r requirements.txt
```

The last command will install the packages found in [`requirements.txt`](https://github.com/DataStories-UniPi/miniDB/blob/master/requirements.txt). MiniDB is based on the following dependencies:
* `tabulate` (for text formatting)
* `graphviz` (for graph visualizations; optional)
* `matplotlib` (for plotting; optional)

Alternatively, the above dependencies can be installed with the following command:
```python
pip install tabulate graphviz matplotlib
```

Linux users can optionally install the `Graphviz` package to visualize graphs:
```bash
sudo apt-get install graphviz
```
Installation instructions for non-Linux users can be found [here](https://graphviz.org/download/).

## Documentation

The file [documentation.pdf](documentation.pdf) contains a detailed description of the miniDB library (in Greek).

## Loading the [smallRelations database](https://www.db-book.com/db6/lab-dir/sample_tables-dir/index.html)

To create a database containing the smallRelations tables and get an interactive shell, run
``` Python
python -i smallRelationsInsertFile.py
```
You can the access the database through the db object that will be available. For example, you can show the contents of the student table by running the following command:
```python
>> db.show_table('student')
```
The database wil be save with the name `smdb`. You can load the database in a separate Python shell by running the following commands:
```python
>> from database import Database
>> db = Database("smdb", load=True)
```

## Contributors
George S. Theodoropoulos, Bill Argiropoulos, Yannis Kontoulis, Apostolos Spanakis-Misirlis, Yannis Theodoridis; Data Science Lab., University of Piraeus.
