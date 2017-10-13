# this is a bunch of macro tests.   If everything passes, there is no output.

import fastsimplexordatastore

size = 64 # block size in Byte
num_blocks = 16 # number of blocks
letterxordatastore = fastsimplexordatastore.XORDatastore(size, num_blocks, "ram", "db_name")

startpos = 0
for char in range(ord("A"), ord("Q")):
	# put 1K of those chars in...
	letterxordatastore.set_data(startpos, chr(char) * size)
	startpos = startpos + size

# can read data out...
assert(letterxordatastore.get_data(size, 1) == 'B')

letterxordatastore.finalize()

# let's create a bitstring that uses A, C, and P.
bitstring = chr(int('10100000', 2)) + chr(int('00000001',2))
xorresult = letterxordatastore.produce_xor_from_bitstring(bitstring)

assert(xorresult[0] == 'R')

xorresult = letterxordatastore.produce_xor_from_multiple_bitstrings(bitstring, 1)

assert(xorresult[0] == 'R')

# let's create a bitstring that uses A, C, and P.
bitstring = chr(int('10100000', 2)) + chr(int('00000001',2)) + chr(int('10000000', 2)) + chr(int('00000000',2)) + chr(int('01001110', 2)) + chr(int('00000001',2))
xorresult = letterxordatastore.produce_xor_from_multiple_bitstrings(bitstring, 3)

assert len(xorresult) == 3*size
assert xorresult[0] == 'R'
assert xorresult[64] == 'A'
assert xorresult[128] == 'V'

letterxordatastore.set_data(10,"Hello there")

mystring = letterxordatastore.get_data(9,13)

assert(mystring == 'AHello thereA')


letterxordatastore.set_data(1,"Hello there"*size)

mystring = letterxordatastore.get_data(size*2 - (size*2 %11) + 1,11)

assert(mystring == "Hello there")

# let's try to read the last bytes of data
mystring = letterxordatastore.get_data(size*15,size)




try:
	letterxordatastore = fastsimplexordatastore.XORDatastore(127, 16)
except TypeError:
	pass
else:
	print "Was allowed to use a block size that isn't a multiple of 64"

try:
	letterxordatastore.set_data(size*16, "hi")
except TypeError:
	pass
else:
	print "Was allowed to write past the end of the datastore"


try:
	letterxordatastore.set_data(size*16, 1)
except TypeError:
	pass
else:
	print "Was allowed to read past the end of the datastore"


for blockcount in [9,14,15,16]:
	letterxordatastore = fastsimplexordatastore.XORDatastore(size, blockcount, "ram", "db_name")

	# is a 0 block the right size?
	assert( len(letterxordatastore.produce_xor_from_bitstring(chr(0)*2)) == size )

	try:
		letterxordatastore.produce_xor_from_bitstring(chr(0)*1)
	except TypeError:
		pass
	else:
		print "didn't detect incorrect (short) bitstring length"


	try:
		letterxordatastore.produce_xor_from_bitstring(chr(0)*3)
	except TypeError:
		pass
	else:
		print "didn't detect incorrect (long) bitstring length"


# test fastsimplexordatastore.do_xor()
from os import urandom

aa=urandom(2**8 + 23)
bb=urandom(2**8 + 23)

cc = fastsimplexordatastore.do_xor(aa,bb)

# slow xor for verification
result = ""
for pos in xrange(len(aa)):
	result = result + chr(ord(aa[pos]) ^ ord(bb[pos]))

# print aa.encode('hex')
# print bb.encode('hex')
# print cc.encode('hex')
# print result.encode('hex')

assert(result == cc)

print "no news is good news. everything OK."
