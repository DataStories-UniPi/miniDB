#run using:
#python3 -i -m src.insert.vsmdb

def main():
    import os, sys
    currentdir = os.path.dirname(os.path.realpath(__file__))
    parentdir = os.path.dirname(currentdir)
    sys.path.append(parentdir)

    from db import database as database
    # create db with name "smdb"
    db = database.Database('vsmdb', load=False)
    # create a single table named "classroom"
    db.create_table('classroom', ['building', 'room_number', 'capacity'], [str,str,int])
    # insert 5 rows
    db.insert('classroom', ['Packard', '101', '500'])
    db.insert('classroom', ['Painter', '514', '10'])
    db.insert('classroom', ['Taylor', '3128', '70'])
    db.insert('classroom', ['Watson', '100', '30'])
    db.insert('classroom', ['Watson', '120', '50'])

if __name__ == '__main__':
    main()
