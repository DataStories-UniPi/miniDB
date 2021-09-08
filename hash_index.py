from database import Database

class HashIndex:
	def __init__(self, b, b_capacity):
		self.b = b
		self.buckets = [None] * b # list of buckets
		self.capacity = b_capacity # max number of entries allowed in each bucket

	def insert(self, value, idx):
		index = hash_function(value,idx) # bucket index to insert the pk value
		self.buckets[index].append(value, idx)

	def hash_function(self, method, value, ptr):
		if method == "modulo":
			index = value % self.buckets
			return index