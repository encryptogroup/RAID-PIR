# this is a bunch of macro tests.   If everything passes, there is no output.

import fastsimplexordatastore

size = 64
letterxordatastore = fastsimplexordatastore.XORDatastore(size, 16)

startpos = 0
for char in range(ord("A"), ord("Q")):
	# put 1K of those chars in...
	letterxordatastore.set_data(startpos, chr(char) * size)
	startpos = startpos + size

# can read data out...
assert(letterxordatastore.get_data(size, 1) == 'B')

# let's create a bitstring that uses A, C, and P.
bitstring = chr(int('10100000', 2)) + chr(int('00000001',2))
xorresult = letterxordatastore.produce_xor_from_bitstring(bitstring)

assert(xorresult[0] == 'R')

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


for blockcount in [9,15,16]:
	letterxordatastore = fastsimplexordatastore.XORDatastore(size, blockcount)

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
