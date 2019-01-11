"""
<Author>
	Justin Cappos

<Start Date>
	May 25th, 2011

<Description>
	A wrapper for a C-based datastore.   This uses objects, etc. to make
	the C interface more Pythonic...

	This is really just a version of the Python datastore with the Python code
	replaced with the C extension.   I left in all of the error checking.

"""

import fastsimplexordatastore_c
import mmapxordatastore_c
import math


def do_xor(bytes_a, bytes_b):

	if type(bytes_a) != bytes or type(bytes_b) != bytes:
		raise TypeError("do_xor must be called with bytes")

	if len(bytes_a) != len(bytes_b):
		raise ValueError("do_xor requires byte types of the same length")

	return fastsimplexordatastore_c.do_xor(bytes_a, bytes_b)


class XORDatastore(object):
	"""
	<Purpose>
		Class that has information for an XORdatastore.   This data structure can
		quickly XOR blocks of data that it stores.   The real work is done in a
		C extension

	<Side Effects>
		None.

	"""

	# this is the private, internal storage area for data...
	ds = None
	dsobj = None

	# these are public so that a caller can read information about a created
	# datastore.   They should not be changed.
	numberofblocks = None
	sizeofblocks = None
	dstype = ""
	use_precomputed_data = 0

	def __init__(self, block_size, num_blocks, dstype, dbname, use_precomputed_data=False):  # allocate
		"""
		<Purpose>
			Allocate a place to store data for efficient XOR.

		<Arguments>
			block_size: the size of each block.   This must be a positive int / long.
									The value must be a multiple of 64

			num_blocks: the number of blocks.   This must be a positive integer
			use_precomputed_data: Use the precomputed 4R data

		<Exceptions>
			TypeError is raised if invalid parameters are given.

		"""

		if type(block_size) != int and type(block_size) != int:
			raise TypeError("Block size must be an integer")

		if block_size <= 0:
			raise TypeError("Block size must be positive")

		if block_size % 64 != 0:
			raise TypeError("Block size must be a multiple of 64 bytes")

		if type(num_blocks) != int and type(num_blocks) != int:
			raise TypeError("Number of blocks must be an integer")

		if num_blocks <= 0:
			raise TypeError("Number of blocks must be positive")


		self.numberofblocks = num_blocks
		self.sizeofblocks = block_size #in byte
		self.use_precomputed_data = int(use_precomputed_data)
		self.dstype = dstype

		if dstype == "mmap":
			self.ds = mmapxordatastore_c.Initialize(block_size, num_blocks, dbname)
			self.dsobj = mmapxordatastore_c
		else: # RAM
			self.ds = fastsimplexordatastore_c.Allocate(block_size, num_blocks)
			self.dsobj = fastsimplexordatastore_c


	def produce_xor_from_bitstring(self, bitstring):
		"""
		<Purpose>
			Returns an XORed block from an XORdatastore.   It will always return
			a string of the size of the datastore blocks

		<Arguments>
			bitstring: bytes that indicates what to XOR.   The length
								 of this string must be ceil(numberofblocks / 8.0).   Extra
								 bits are ignored (e.g. if there are 10 blocks, the last
								 six bits are ignored).

		<Exceptions>
			TypeError is raised if the bitstring is invalid

		<Returns>
			The XORed block.

		"""
		if type(bitstring) != bytes:
			raise TypeError("bitstring must be of type bytes")

		if len(bitstring) != math.ceil(self.numberofblocks/8.0):
			raise TypeError("bitstring is not of the correct length")

		return self.dsobj.Produce_Xor_From_Bitstring(self.ds, bitstring, self.use_precomputed_data)


	def produce_xor_from_multiple_bitstrings(self, bitstring, num_strings):
		"""
		<Purpose>
			Returns multiple XORed block from an XORdatastore. It will always return
			a string of the size of the datastore blocks times num_strings

		<Arguments>
			bitstring: concatenated string of bits that indicates what to XOR. The length
								 of this string must be numberofblocks * num_strings / 8.   Extra
								 bits are ignored (e.g. if there are 10 blocks, the last
								 six bits are ignored).
			num_strings: the number of requests in bitstring

		<Exceptions>
			TypeError is raised if the bitstring is invalid

		<Returns>
			The XORed block.

		"""
		if type(bitstring) != bytes:
			raise TypeError("bitstring must be of type bytes")

		if len(bitstring) != math.ceil(self.numberofblocks / 8.0)*num_strings :
			raise TypeError("bitstring is not of the correct length")

		return self.dsobj.Produce_Xor_From_Bitstrings(self.ds, bitstring, num_strings, self.use_precomputed_data)


	def set_data(self, offset, data_to_add):
		"""
		<Purpose>
			Sets the raw data in an XORdatastore.   It ignores block layout, etc.

		<Arguments>
			offset: this is a non-negative integer that must be less than the
							numberofblocks * blocksize.

			data_to_add: the string that should be added.   offset + len(data_to_add)
								must be less than the numberofblocks * blocksize.

		<Exceptions>
			TypeError if the arguments are the wrong type or have invalid values.

		<Returns>
			None

		"""
		if type(offset) != int and type(offset) != int:
			raise TypeError("Offset must be an integer")

		if offset < 0:
			raise TypeError("Offset must be non-negative")

		if type(data_to_add) != bytes:
			raise TypeError("Data_to_add to XORdatastore must be bytes.")

		if offset + len(data_to_add) > self.numberofblocks * self.sizeofblocks:
			raise TypeError("Offset + added data overflows the XORdatastore")

		return self.dsobj.SetData(self.ds, offset, data_to_add)


	def get_data(self, offset, quantity):
		"""
		<Purpose>
			Returns raw data from an XORdatastore. It ignores block layout, etc.

		<Arguments>
			offset: this is a non-negative integer that must be less than the numberofblocks * blocksize.

			quantity: quantity must be a positive integer. offset + quantity must be less than the numberofblocks * blocksize.

		<Exceptions>
			TypeError if the arguments are the wrong type or have invalid values.

		<Returns>
			A string containing the data.

		"""
		if type(offset) != int and type(offset) != int:
			raise TypeError("Offset must be an integer")

		if offset < 0:
			raise TypeError("Offset must be non-negative")

		if type(quantity) != int and type(quantity) != int:
			raise TypeError("Quantity must be an integer")

		if quantity <= 0:
			raise TypeError("Quantity must be positive")

		if offset + quantity > self.numberofblocks * self.sizeofblocks:
			raise TypeError("Quantity + offset is larger than XORdatastore")

		return self.dsobj.GetData(self.ds, offset, quantity)

	def finalize(self):
		"""
		<Purpose>
			Does the preprocessing

		<Arguments>
			None

		<Exceptions>
			None

		<Returns>
			None

		"""
		self.dsobj.DoPreprocessing(self.ds)

	def __del__(self):   # deallocate
		"""
		<Purpose>
			Deallocate the XORdatastore

		<Arguments>
			None

		<Exceptions>
			None

		"""
		# if there is an error, this might be an uninitialized object...
		if self.ds != None:
			self.dsobj.Deallocate(self.ds)
