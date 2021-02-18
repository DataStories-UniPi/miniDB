import pstats
import cProfile
from pstats import SortKey
import io
from tabulate import tabulate
import re
import table
from database import Database
from btree import Node, Btree

# Class miniDBProfiler contains methods to profile given command and show results
class miniDBProfiler(object):

    # initialize class
    def __init__(self):
        ok = "ok"

    # method profileMiniDB takes arguments toBeProfiled which is the given command as a string
    # and db which is the database in which the command is supposed to work on
    def profileMiniDB(self, toBeProfiled, db):

        # using cProfile we profile the execution of the command and dump it into file output.pstats
        with cProfile.Profile() as pr:
            pr.enable()
            exec(toBeProfiled)
            pr.dump_stats('output.pstats')

        # create readable file output.txt with information from output.pstats
        stream = open('output.txt', 'w')
        stats = pstats.Stats('output.pstats',stream=stream)
        stats.print_stats()
        stream.close()

        # call createPrint() to show results in terminal.
        self.createPrint()

    # static method createPrint() creates and prints output in terminal.
    @staticmethod
    def createPrint():

        # create a list with lines contained in output.txt
        list_of_results = []
        with open('output.txt', 'r') as a:

            # first lines which contain datetime, function calls and listing are kept as they are
            dateTime = a.readline().rstrip()
            a.readline().rstrip()
            functionCalls = a.readline().rstrip()
            a.readline().rstrip()
            listing = a.readline().rstrip()

            # filter and keep only lines that contain miniDB in their path
            # so that we only see information about miniDB methods
            for line in a:
                if "miniDB" in line:
                    stripped_line=line.strip()
                    line_list=stripped_line.strip()
                    final=re.sub(" "," ", line_list).split()
                    list_of_results.append(final)
            a.close()

        # print a nice table with the results.
        print("\n")
        print("--------------------------------------------------------------------------------")
        print("         "+dateTime)
        print(functionCalls)
        print("         "+"From Which miniDB: \n")
        # print("      "+listing+"\n")
        print(tabulate(list_of_results,headers=["ncalls","tottime","percall","cumtime","percall","filename"]))
