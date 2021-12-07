<p align=center>
  <img width="450" alt="mdblogo" src="https://user-images.githubusercontent.com/15364873/144466217-8430758c-63e4-4176-8f0f-30e2055f858d.png">
</p>

# miniDB

The miniDB project is a minimal and easy to expand and develop for RMDBS tool, written exclusivelly in Python 3. MiniDB's main goal is to provide the user with as much functionality as possible while being easy to understand and even easier to expand. Thus, miniDB's primary market are students and researchers that want to work with a tool that they can understand through and through, while being able to implement additional features as quickly as possible.

## Installation

Python 3.9 or newer is needed. To download and build the project run:

```bash
git clone https://github.com/DataStories-UniPi/miniDB.git
cd miniDB
pip install -r requirements.txt
```

The last command will install the packages found in [`requirements.txt`](https://github.com/DataStories-UniPi/miniDB/blob/master/requirements.txt). MiniDB is based on the following dependencies:
* `tabulate` (for text formatting)
* `prompt_toolkit` (for sql compiler input)
* `graphviz` (for graph visualizations; optional)
* `matplotlib` (for plotting; optional)

Linux users will need to install the `Graphviz` package to visualize graphs:
```bash
sudo apt-get install graphviz
```
Installation instructions for non-Linux users can be found [here](https://graphviz.org/download/).

## Documentation

The file [documentation.pdf](documentation.pdf) contains a detailed description of the miniDB library (in Greek).

## Loading the [smallRelations database](https://www.db-book.com/db6/lab-dir/sample_tables-dir/index.html)

To create a database called _"smdb"_ containing the smallRelations tables and get an interactive shell, run
```
DB=smdb SQL=sql_files/smallRelationsInsertFile.sql python3.9 mdb.py
```
You can then access the database that is saved. You can either open an interactive interpreter with the following command:
```
DB=smdb python3.9 mdb.py
```
or run a specific sql file with multiple commands using the following syntax:
```
DB=smdb SQL=YOUR_FILE python3.9 mdb.py
```

## Contributors
George S. Theodoropoulos, Bill Argiropoulos, Yannis Kontoulis, Apostolos Spanakis-Misirlis, Yannis Theodoridis; Data Science Lab., University of Piraeus.
