import heapq
import shutil
import os
import ast
import sys

sys.path.append(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/miniDB')

from misc import reverse_op
from table import Table



class Inlj:
    def __init__(self, condition, left_table, right_table, index, index_saved):
        self.left_table = left_table
        self.right_table = right_table
        self.condition = condition
        self.join_table = None
        self.results = None
        self.index = index
        self.index_saved = index_saved

    def join(self):
        # Get the column of the left and right tables and the operator, from the condition of the join
        column_name_left, operator, column_name_right = Table()._parse_condition(self.condition, join=True)
        
        reversed = False
        # If we have the index of the left table, reverse the order of the tables
        if(self.index_saved=='left'):
            self.right_table, self.left_table = self.left_table, self.right_table
            column_name_left, column_name_right = column_name_right, column_name_left
            reversed = True
        
        # Try to find the left column, as even if a reverse took place, it is the only one needed
        # If it fails, raise exception
        try:
            self.column_index_left = self.left_table.column_names.index(column_name_left)
        except:
            raise Exception(f'Column "{column_name_left}" doesn\'t exist in the left table. Valid columns: {self.left_table.column_names}.')

        # Create the names that appear over the tables when the final joined table is presented to the user
        left_names = [f'{self.left_table._name}.{colname}' if self.left_table._name!='' else colname for colname in self.left_table.column_names]
        right_names = [f'{self.right_table._name}.{colname}' if self.right_table._name!='' else colname for colname in self.right_table.column_names]

        join_table_colnames = left_names + right_names if not reversed else right_names + left_names
        join_table_coltypes = self.left_table.column_types + self.right_table.column_types if not reversed else self.right_table.column_types + self.left_table.column_types
        self.join_table = Table(name='', column_names=join_table_colnames, column_types=join_table_coltypes)

        # The operator needs to be reversed as we search based on the elements of the index.
        # For example, if A > B and we search based on B, we need to search for B < A
        operator = reverse_op(operator) if self.index_saved == 'right' else operator

        # Implementation of the index-nested-loop join
        # If the tables had been reversed in the beginning, then the joined table appears
        # with the tables shown in the order they appeared in the query
        for row_left in self.left_table.data:
            # The value that will be searched for in the index
            left_value = row_left[self.column_index_left]
            self.results = self.index.find(operator, left_value)
            if len(self.results) > 0:
                for element in self.results:
                    self.join_table._insert(row_left + self.right_table.data[element] if not reversed else self.right_table.data[element] + row_left)

        return self.join_table

class Smj:

    def __init__(self, condition, left_table, right_table):
        self.left_table = left_table
        self.right_table = right_table
        self.condition = condition

    def join(self):
        # Get the column of the left and right tables and the operator, from the condition of the join
        column_name_left, operator, column_name_right = Table()._parse_condition(self.condition, join=True)

        if(operator != "="):
            raise Exception("Sort-Merge Join is used when the condition operator is '='.")

        # Create a temporary folder for the external sort to happen. The folder will be deleted in the end
        os.makedirs('miniDB/externalSortFolder', exist_ok=True)

        # Create the names that appear over the tables when the final joined table is presented to the user
        left_names = [f'{self.left_table._name}.{colname}' if self.left_table._name!='' else colname for colname in self.left_table.column_names]
        right_names = [f'{self.right_table._name}.{colname}' if self.right_table._name!='' else colname for colname in self.right_table.column_names]

        # Write all the records of the right table to a local file in the following format:
        # 'column_name_value [whole record]'
        # Use special character '@@@' to represent spaces as spaces break the code.
        with open('miniDB/externalSortFolder/rightTableFile', 'w+') as rt:
            for row in self.right_table.data:
                rt.write(f'{row[self.right_table.column_names.index(column_name_right)]} {str(row).replace(" ","@@@")}\n')
        
        # Same for the left lable
        with open('miniDB/externalSortFolder/leftTableFile', 'w+') as lt:
            for row in self.left_table.data:
                lt.write(f'{row[self.left_table.column_names.index(column_name_left)]} {str(row).replace(" ","@@@")}\n')
        
        # Create an ExternalMergeSort object and sort both right table and left table local files
        ems = self.ExternalMergeSort()
        ems.runExternalSort('rightTableFile')
        # Re-initialization of all values
        ems = self.ExternalMergeSort()
        ems.runExternalSort('leftTableFile')

        # Now there are sorted versions of the local files, so the initial ones can be removed
        os.remove('miniDB/externalSortFolder/rightTableFile')
        os.remove('miniDB/externalSortFolder/leftTableFile')

        # This does the final merge on sort-merge join
        with open('miniDB/externalSortFolder/sorting of rightTableFile', 'r') as right, open('miniDB/externalSortFolder/sorting of leftTableFile', 'r') as left, open('miniDB/externalSortFolder/final', 'w+') as final:
            mark = None #Used to return to previous values of the file
            l = None
            r = None

            # The algorithm runs until EOF of the left table file
            while l != '':
                try:
                    # If the mark is non-existent, set it equal to the current line of the right table sorted file
                    # While both files' column_values aren't equal, progress the current lines
                    if mark is None:
                        mark = right.tell()
                        l = left.readline()
                        r = right.readline()
                        while l.split()[0] < r.split()[0]:
                            l = left.readline()
                        while l.split()[0] > r.split()[0]:
                            mark = right.tell()
                            r = right.readline()
                    
                    # Now that the column_values are equal save both records to the final, joined_tables local file
                    # Then progress the right table's current line and continue with the procedure
                    if l.split()[0] == r.split()[0]:
                        final.write(l.replace("\n","")[l.index("['"):] + " " + r.replace("\n","")[r.index("['"):] + '\n')
                        r = right.readline()
                    
                    # Else, if left_value isn't equal to right_value after having found at least one equality of column_values
                    # return right table's current line to the mark, as the algorithm dictates
                    else:
                        right.seek(mark)
                        mark = None
                # Finally, if the right file reaches EOF and IndexError happens, return right table's current line to the mark
                except IndexError:
                    right.seek(mark)
                    mark = None
        
        # Now that the final joined file exists, the sorted files are not needed and are thus deleted
        os.remove('miniDB/externalSortFolder/sorting of rightTableFile')
        os.remove('miniDB/externalSortFolder/sorting of leftTableFile')

        join_table_name = ''
        join_table_colnames = left_names + right_names
        join_table_coltypes = self.left_table.column_types + self.right_table.column_types
        join_table = Table(name=join_table_name, column_names=join_table_colnames, column_types= join_table_coltypes)

        # Save merged file first. The hypothesis is that the RAM cannot fit the file, thus we have it saved
        # However we load the file to display it like this, might need to be changed in the future
        with open('miniDB/externalSortFolder/final', 'r') as f:
            for line in f:
                records = line.split()
                # ast.literal_eval creates the list [a,b,c] from the string '[a,b,c]'
                join_table._insert(ast.literal_eval(records[0].replace('@@@', ' ')) + ast.literal_eval(records[1].replace('@@@', ' ')))

        # Finally, the final file and the externalSortFolder are not needed, as the joined table
        # exists in a variable and can be presented to the user
        os.remove(f'{os.getcwd()}/miniDB/externalSortFolder/final')
        os.rmdir(f'{os.getcwd()}/miniDB/externalSortFolder')
        
        return join_table

    class ExternalMergeSort:
        # Total number of split files
        numFiles = 0
        # Total number of numbers in the first file
        sumFiles = 0
        # File name, so as to recognize the sorted file
        startingFileName = ''

        # Sort the given array with the merge sort algorithm
        def mergeSort(self, arr):
            if len(arr) > 1:
                mid = len(arr)//2
                L, R = arr[:mid], arr[mid:]

                # Recursive call of merge sort
                self.mergeSort(L)
                self.mergeSort(R)
        
                i = j = k = 0

                while i < len(L) and j < len(R):
                    if L[i] < R[j]:
                        arr[k] = L[i]
                        i += 1
                    else:
                        arr[k] = R[j]
                        j += 1
                    k += 1

                while i < len(L):
                    arr[k] = L[i]
                    i += 1
                    k += 1
        
                while j < len(R):
                    arr[k] = R[j]
                    j += 1
                    k += 1

        # Function to split the big file into smaller chunks of size specified by the user
        def splitFile(self, largeFile, chunkSize:int):
            # Variables used throughout the class
            self.startingFileName = largeFile
            self.numFiles = 1

            # Split the file in chunks of chunkSize bytes
            with open(f'miniDB/externalSortFolder/{largeFile}') as f:
                chunk = f.readlines(chunkSize)
                while chunk:
                    os.makedirs(os.path.dirname(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{self.numFiles}'), exist_ok=True)
                    with open(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{self.numFiles}', 'w+') as chunk_file:
                        for el in chunk:
                            chunk_file.write(el)

                    chunk = f.readlines(chunkSize)
                    self.numFiles += 1

            return self.numFiles

        # Function to sort a chunk of the starting file using merge sort
        def sortSmallFile(self, fileToBeSorted):
            arr = []

            with open(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{fileToBeSorted}', 'r') as fts:
                # If the contents of the file are integers
                try:
                    with open(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{fileToBeSorted}', 'r') as fts:
                        arr = list(map(int, fts.read().splitlines()))
                # If the contents are alphanumeric values
                except:
                    with open(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{fileToBeSorted}', 'r') as fts:
                        arr = list(map(str, fts.read().splitlines()))
                
                self.sumFiles += len(arr)
                self.mergeSort(arr)

            with open(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{fileToBeSorted}', 'w') as fts:
                for el in arr:
                    fts.write(f'{el}\n')

        # K-Way Merge with priority queue implementation
        def k_wayMerge(self, number):
            # Create dictionary of files opened. Open all the files
            # That will be merged
            fileNames = {}
            for i in range(1, number):
                fileNames[i] = open(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/{i}', 'r')

            output = []

            # (X,Y) where
            # X is the value of the element and
            # Y is the key of the file in fileName
            try:
                pq = [(int(fileNames[i].readline().replace('\n', '')), i) for i in range(1, len(fileNames) + 1)]
            except:
                for i in range(1, len(fileNames) + 1):
                    fileNames[i].seek(0)
                pq = [(fileNames[i].readline().replace('\n', ''), i) for i in range(1, len(fileNames) + 1)]

            # Create heap for the external merge sort
            heapq.heapify(pq)

            while len(output) < self.sumFiles:
                elem, file_key = heapq.heappop(pq)
                output.append(elem)
                next = fileNames[file_key].readline().replace('\n', '')

                # When on EOF, an empty string will be returned
                # So if the value is not an empty string, add the integer to the heap
                if next != '':
                    try:
                        heapq.heappush(pq, (int(next), file_key))
                    except:
                        heapq.heappush(pq, (next, file_key))

            with open(f'miniDB/externalSortFolder/sorting of {self.startingFileName}', 'w+') as sf:
                for el in output:
                    sf.write(f'{el}\n')

            return output

        def runExternalSort(self, filename):
            # 30 is just an example
            self.splitFile(filename, 30)

            for i in range(1, self.numFiles):
                self.sortSmallFile(i)

            self.k_wayMerge(self.numFiles)
            
            # After the k-way Merge is completed, remove the folder containing the temporary split files
            shutil.rmtree(f'miniDB/externalSortFolder/tempSplitFiles {self.startingFileName}/')
