#!/usr/bin/env python3
# this is a bunch of macro tests.

import fastsimplexordatastore

#changing these numbers will require changes to the asserts below!
size = 64 # block size in Byte
num_blocks = 16 # number of blocks

letterxordatastore = fastsimplexordatastore.XORDatastore(size, num_blocks, "ram", "db_name")

startpos = 0
for char in range(65, 65 + num_blocks):
	# put blocks of those chars in...
	letterxordatastore.set_data(startpos, bytes([char]) * size)
	startpos = startpos + size

# can read data out...
assert letterxordatastore.get_data(size, 1) == b'B'

letterxordatastore.finalize()

# let's create a bitstring that uses A, C, and P.
# bitstring = chr(int('10100000', 2)) + chr(int('00000001',2))
bitstring = b'\xa0\x01'
xorresult = letterxordatastore.produce_xor_from_bitstring(bitstring)

assert xorresult[0] == ord('R')

xorresult = letterxordatastore.produce_xor_from_multiple_bitstrings(bitstring, 1)

assert xorresult[0] == ord('R')

# let's create a bitstring that uses A, C, and P.
bitstring = bytes([int('10100000', 2)]) + bytes([int('00000001', 2)]) + bytes([int('10000000', 2)]) + bytes([int('00000000', 2)]) + bytes([int('01001110', 2)]) + bytes([int('00000001', 2)])
xorresult = letterxordatastore.produce_xor_from_multiple_bitstrings(bitstring, 3)

assert len(xorresult) == 3*size
assert xorresult[0] == ord('R')
assert xorresult[64] == ord('A')
assert xorresult[128] == ord('V')

letterxordatastore.set_data(10, b"Hello there")

mystring = letterxordatastore.get_data(9, 13)

assert mystring == b'AHello thereA'


letterxordatastore.set_data(1, b"Hello there"*size)

mystring = letterxordatastore.get_data(size*2 - (size*2 %11) + 1,11)

assert mystring == b"Hello there"

# let's try to read the last bytes of data
mystring = letterxordatastore.get_data(size*15,size)


try:
	letterxordatastore = fastsimplexordatastore.XORDatastore(127, 16)
except TypeError:
	pass
else:
	print("Was allowed to use a block size that isn't a multiple of 64")

try:
	letterxordatastore.set_data(size*16, "hi")
except TypeError:
	pass
else:
	print("Was allowed to write past the end of the datastore")


try:
	letterxordatastore.set_data(size*16, 1)
except TypeError:
	pass
else:
	print("Was allowed to read past the end of the datastore")


for blockcount in [9,14,15,16]:
	letterxordatastore = fastsimplexordatastore.XORDatastore(size, blockcount, "ram", "db_name")

	# is a 0 block the right size?
	assert len(letterxordatastore.produce_xor_from_bitstring(b'\x00\x00')) == size

	try:
		letterxordatastore.produce_xor_from_bitstring(b'\x00')
	except TypeError:
		pass
	else:
		print("didn't detect incorrect (short) bitstring length")


	try:
		letterxordatastore.produce_xor_from_bitstring(b'\x00\x00\x00')
	except TypeError:
		pass
	else:
		print("didn't detect incorrect (long) bitstring length")

# test fastsimplexordatastore.do_xor()
from os import urandom

xorlen = 2**10 + 23
aa=urandom(xorlen)
bb=urandom(xorlen)

cc = fastsimplexordatastore.do_xor(aa,bb)

# slow xor for verification
result = b""
for pos in range(len(aa)):
	result = result + bytes([aa[pos] ^ bb[pos]])

# print(aa)
# print(bb)
# print(cc)
# print(result)

assert result == cc

print("no news is good news. everything OK.")
