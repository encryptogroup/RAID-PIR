"""
<Author>
	Daniel Demmler
	(inspired from upPIR by Justin Cappos et al.)
	(inspired from a previous version by Geremy Condra)

<Date>
	January 2019

<Description>
	Library code that emulates the upPIR fastsimpleXORdatastore in C. This is likely to only
	be used for testing.

"""

import math

# alternative for fast XOR
import numpy


def do_xor(bytes_a, bytes_b):
	"""
	<Purpose>
		Produce the XOR of two equal length strings

	<Arguments>
		bytes_a, bytes_b: the strings to XOR

	<Exceptions>
		ValueError if the strings are of unequal lengths
		TypeError if the strings are not strings

	<Returns>
		The XORed result.
	"""
	if type(bytes_a) != bytes or type(bytes_b) != bytes:
		raise TypeError("do_xor must be called with bytes")

	if len(bytes_a) != len(bytes_b):
		raise ValueError("do_xor requires byte arrays of the same length")

	n_a = numpy.frombuffer(bytes_a, dtype='uint8')
	n_b = numpy.frombuffer(bytes_b, dtype='uint8')

	return (n_a ^ n_b).tostring()


def do_xor_blocks(bytes_a, bytes_b):
	"""
	<Purpose>
		Produce the XOR of two equal length strings with length of a multiple of 8 byte / 64 bit

	<Arguments>
		bytes_a, bytes_b: the strings to XOR, both equal length, multiple of 64 bit

	<Exceptions>
		ValueError if the strings are of unequal lengths
		TypeError if the strings are not strings

	<Returns>
		The XORed result.
	"""
	if type(bytes_a) != bytes or type(bytes_b) != bytes:
		raise TypeError("do_xor_blocks must be called with bytes")

	if len(bytes_a) != len(bytes_b):
		raise ValueError("do_xor_blocks requires byte arrays of the same length")

	n_a = numpy.frombuffer(bytes_a, dtype='uint64')
	n_b = numpy.frombuffer(bytes_b, dtype='uint64')

	return (n_a ^ n_b).tostring()


def do_xor_old(string_a, string_b):
	"""
	<Purpose>
		Produce the XOR of two equal length strings

	<Arguments>
		string_a, string_b: the strings to XOR

	<Side Effects>
		None

	<Exceptions>
		ValueError if the strings are of unequal lengths
		TypeError if the strings are not strings

	<Returns>
		The XORed result.
	"""
	if type(string_a) != str or type(string_b) != str:
		raise TypeError("do_xor called with a non-string")

	if len(string_a) != len(string_b):
		raise ValueError("do_xor requires strings of the same length")

	result = ''

	for pos in range(len(string_a)):
		# add the XOR of the two characters.   I must convert them to do so...
		result = result + chr(ord(string_a[pos]) ^ ord(string_b[pos]))

	return result


class XORDatastore(object):
	"""
	<Purpose>
		Class that has information for an XORdatastore. This data structure can
		quickly XOR blocks of data that it stores.

	<Side Effects>
		None.

	<Example Use>
		# build an XORdatastore with 16 1KB blocks.
		letterxordatastore = XORDatastore(1024, 16)

		# fill the XORdatastore with 1KB blocks with "A"-"P"
		startpos = 0
		for char in range(ord("A"), ord("Q")):
			# put 1K of those chars in...
			letterxordatastore.set_data(startpos, chr(char) * 1024)
			startpos = startpos + 1024

		# can read data out...
		print letterxordatastore.get_data(2000, 1)
		# Should print 'B' as this is the 1 thousandth letter...

		# let's create a bitstring that uses A, C, and P.
		bitstring = chr(int('10100000', 2)) + chr(int('00000001', 2))
		xorresult = letterxordatastore.produce_xor_from_bitstring(bitstring)

		print xorresult[0]
		# this should be 'A' ^ 'C' ^ 'P' which is 'R'

	"""

	# this is the private, internal storage area for data...
	_blocks = []

	# these are public so that a caller can read information about a created
	# datastore.   They should not be changed.
	numberofblocks = None
	sizeofblocks = None

	def __init__(self, block_size, num_blocks, dstype, dbname, use_precomputed_data=False):  # allocate
		"""
		<Purpose>
			Allocate a place to store data for efficient XOR.

		<Arguments>
			block_size: the size of each block.   This must be a positive int / long.
									The value must be a multiple of 64

			num_blocks: the number of blocks.   This must be a positive integer

		<Exceptions>
			TypeError is raised if invalid parameters are given.

		"""

		if type(block_size) != int and type(block_size) != int:
			raise TypeError("Block size must be an integer")

		if block_size <= 0:
			raise TypeError("Block size must be positive")

		if block_size % 64 != 0:
			raise TypeError("Block size must be a multiple of 64")

		if type(num_blocks) != int and type(num_blocks) != int:
			raise TypeError("Number of blocks must be an integer")

		if num_blocks <= 0:
			raise TypeError("Number of blocks must be positive")


		self.numberofblocks = num_blocks
		self.sizeofblocks = block_size

		# let's create appropriately sized strings of all zero characters.   This
		# will serve as padding if there are 'gaps' in the data added.
		self._blocks = []
		for _ in range(self.numberofblocks):
			self._blocks.append(b'\x00' * self.sizeofblocks)


	def produce_xor_from_bitstring(self, bitstring):
		"""
		<Purpose>
			Returns an XORed block from an XORdatastore.   It will always return
			a string of the size of the datastore blocks

		<Arguments>
			bitstring: a string of bits that indicates what to XOR.   The length
								 of this string must be ceil(numberofblocks / 8.0).   Extra
								 bits are ignored (e.g. if are 10 blocks, the last
								 six bits are ignored).

		<Exceptions>
			TypeError is raised if the bitstring is invalid

		<Returns>
			The XORed block.

		"""
		if type(bitstring) != bytes:
			raise TypeError("bitstring must be bytes")

		if len(bitstring) != math.ceil(self.numberofblocks / 8.0):
			raise TypeError("bitstring is not of the correct length")

		# start with an empty string of the right size...
		currentblock = b'\x00' * self.sizeofblocks

		# This is the block we are looking at now
		currentblocknumber = 0

		for thisbyte in bitstring:

			for bitnumber in range(0, 8):

				# find the value we need to & the data with.
				bitvalue = 2 ** (7 - bitnumber)

				# If it's a one...
				if thisbyte & bitvalue:

					# ... and we're not past the end of the string...
					if currentblocknumber < self.numberofblocks:
						# ... do the xor
						currentblock = do_xor_blocks(currentblock, self._blocks[currentblocknumber])

				# regardless, we are done with this block.
				currentblocknumber = currentblocknumber + 1

		# let's return the result!
		return currentblock


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

		(startblock, startoffset) = self._find_blockloc_from_offset(offset)
		(endblock, endoffset) = self._find_blockloc_from_offset(offset + len(data_to_add))

		# Case 1: this does not cross blocks
		if startblock == endblock:

			# insert the string into the block...
			self._blocks[startblock] = self._blocks[startblock][:startoffset] + data_to_add + self._blocks[startblock][endoffset:]
			return

		# Case 2: this crosses blocks (or ends a single block)
		# we'll add the string starting with the first block...
		self._blocks[startblock] = self._blocks[startblock][:startoffset] + data_to_add[:self.sizeofblocks - startoffset]

		amountadded = self.sizeofblocks - startoffset

		# now add in the 'middle' blocks.   This is all of the blocks after the start and before the end
		for currentblock in range(startblock + 1, endblock):
			self._blocks[currentblock] = data_to_add[amountadded:amountadded + self.sizeofblocks]
			amountadded = amountadded + self.sizeofblocks


		# finally, add the end block.   Add everything left over and pad with the previous block data.
		if endoffset > 0:
			self._blocks[endblock] = data_to_add[amountadded:] + self._blocks[endblock][len(data_to_add) - amountadded:]


	def get_data(self, offset, quantity):
		"""
		<Purpose>
			Returns raw data from an XORdatastore.   It ignores block layout, etc.

		<Arguments>
			offset: this is a non-negative integer that must be less than the
							numberofblocks * blocksize.

			quantity: quantity must be a positive integer.   offset + quantity
								must be less than the numberofblocks * blocksize.

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


		# Let's get the block information
		(startblock, startoffset) = self._find_blockloc_from_offset(offset)
		(endblock, endoffset) = self._find_blockloc_from_offset(offset + quantity)

		# Case 1: this does not cross blocks
		if startblock == endblock:
			return self._blocks[startblock][startoffset:endoffset]

		# Case 2: this crosses blocks

		# we'll build up the string starting with the first block...
		currentstring = self._blocks[startblock][startoffset:]

		# now add in the 'middle' blocks.   This is all of the blocks
		# after the start and before the end
		for currentblock in range(startblock + 1, endblock):
			currentstring += self._blocks[currentblock]

		# this check is needed because we might be past the last block.
		if endoffset > 0:
			# finally, add the end block.
			currentstring += self._blocks[endblock][:endoffset]

		# and return the result
		return currentstring


	def _find_blockloc_from_offset(self, offset):
		# Private helper function that translates an offset into (block, offset)
		assert offset >= 0
		assert offset <= self.numberofblocks * self.sizeofblocks

		return (int(offset / self.sizeofblocks), offset % self.sizeofblocks)


	def __del__(self):  # deallocate
		"""
		<Purpose>
			Deallocate the XORdatastore

		<Arguments>
			None

		<Exceptions>
			None

		"""
		# Other implementations may need to do something here.   We need not...
		pass
