import heapq
import os
import shutil

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