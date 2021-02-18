---------------------------------------------------------------------------------------------------------------

miniDBProfiler use and information.
Project Contributors: Dimitriadou Eleftheria P18035 and Panagiotis Kalogeridis P18052.

---------------------------------------------------------------------------------------------------------------

To profile a miniDB command:

1.from miniDBProfiler import miniDBProfiler
2.create a miniDBProfiler instance: mp=miniDBProfiler()
3.use method profileMiniDB() to profile a command: mp.profileMiniDB("your_command",your_db)

example to create new table in a database loaded on an instance called db1:
mp.profileMiniDB("db.create_table('new_table',['col1','col2'],[str,str])", db1)

Note: in the command string you must use db as a database instance name(ex. db.your_command, db=your_command) 
and then give the database instance name you are using as the second argument.

---------------------------------------------------------------------------------------------------------------

When you use miniDBProfiler, the command you profile will be executed and you will see its results.
Under the result you will see the profiling results. 
Those are the date and time the profiling happened, the number of all function calls used for the execution,
total time of execution followed by in-depth information on miniDB methods called.

Specifically:
ncalls = number of times a method was called
tottime = total time of execution all times a method was called
percall = time for one call (tottime/ncalls)
cumtime = total time spent on function and all subfunctions (accurate even for recursive functions)
percall = time for primitive functions for one call(cumtime/primitive calls)
filename = function data (path, name of file and name of function)

---------------------------------------------------------------------------------------------------------------

A file named output.txt is created and saved with information about all functions called (not only miniDB's)
and can be accessed any time (Though it's re-written every time you use miniDBProfiler).

You can also only call function createPrint() to see only miniDB profile that was saved the last time you 
used miniDBProfiler: database_instance.createPrint()
example: mp.createPrint()

---------------------------------------------------------------------------------------------------------------

Other Comments:

When you use miniDBProfiler and create a new database, you have to load it to get the result in a database
instance.

---------------------------------------------------------------------------------------------------------------





