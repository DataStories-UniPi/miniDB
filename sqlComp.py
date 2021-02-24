from database import Database

print("\n\n--SQL to miniDB interpreter--")
#--CREATE DATABASE
def create_db(name):
    db = Database(name)

#--DROP DATABASE
def drop_db(name):
    db = Database(name, True)
    db.drop_db()
    del db

#--LOAD DATABASE
def load_db(name):
    db = Database(name, True)


#--SAVE DATABASE
def save_db(name):
    db = Database(name, True)
    db.save()
