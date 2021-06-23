import os
import subprocess

class TestInsert:

    def test_vsmdb(self):
        cmd = 'python3 -m src.insert.vsmdb'
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        out, err = p.communicate()
        result = out.split()
        print("Test")
        print(os.getcwd())
        print(os.path.isdir('dbdata/vsmdb_db'))
        assert os.path.isdir('dbdata/vsmdb_db') == True

    def test_smallRelationsInsertFile(self):
        cmd = 'python3 -m src.insert.smallRelationsInsertFile'
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        out, err = p.communicate()
        result = out.split()
        print("Test")
        print(os.getcwd())
        print(os.path.isdir('dbdata/smdb_db'))
        assert os.path.isdir('dbdata/smdb_db') == True

    def test_smallRelationsInsertFileBulk(self):
        cmd = 'python3 -m src.insert.smallRelationsInsertFileBulk'
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        out, err = p.communicate()
        result = out.split()
        print("Test")
        print(os.getcwd())
        print(os.path.isdir('dbdata/smdb_db'))
        assert os.path.isdir('dbdata/smdb_db') == True