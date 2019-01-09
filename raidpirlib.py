"""
<Author>
	Daniel Demmler
	(inspired from upPIR by Justin Cappos et al.)
	(inspired from a previous version by Geremy Condra)

<Date>
	December 2014

<Description>
	Lots of helper code for RAID-PIR. Much of this code will be used multiple
	places, but some many not.   Anything that is at least somewhat general will
	live here.

"""

import sys

# used for os.path.exists, os.path.join and os.walk
import os

# only need ceil
import math

import socket

# use this to turn the stream abstraction into a message abstraction...
import session

try:
	# for packing more complicated messages
	import msgpack
except ImportError:
	print "Requires MsgPack module (http://msgpack.org/)"
	sys.exit(1)

# Check the python version.   It's pretty crappy to do this from a library,
# but it's an easy way to check this universally
if sys.version_info[0] != 2 or sys.version_info[1] != 7:
	print "Requires Python 2.7"
	sys.exit(1)

import hashlib

from Crypto.Cipher import AES
from Crypto.Util import Counter

import time
_timer = time.time

pirversion = "v0.9.4"

# Exceptions...
class FileNotFound(Exception):
	"""The file could not be found"""

class IncorrectFileContents(Exception):
	"""The contents of the file do not match the manifest"""


# these keys must exist in a manifest dictionary.
_required_manifest_keys_regular = ['manifestversion', 'blocksize', 'blockcount', 'blockhashlist', 'hashalgorithm', 'vendorhostname', 'vendorport', 'fileinfolist']
_required_manifest_keys = ['manifestversion', 'blocksize', 'blockcount', 'hashalgorithm', 'vendorhostname', 'vendorport', 'fileinfolist']


# the original implementation, used in mirrors that hold data in RAM
def _compute_block_hashlist_fromdatastore(xordatastore, blockcount, blocksize, hashalgorithm):
	"""private helper, used both the compute and check hashes"""

	currenthashlist = []

	# skip hash calculation if that is desired
	if hashalgorithm == 'noop' or hashalgorithm == 'none' or hashalgorithm == None:
		for _ in xrange(blockcount):
			currenthashlist.append('')
		return currenthashlist

	# Now I'll check the blocks have the right hash...
	for blocknum in xrange(blockcount):
		# read the block ...
		thisblock = xordatastore.get_data(blocksize * blocknum, blocksize)
		# ... and check its hash
		currenthashlist.append(find_hash(thisblock, hashalgorithm))

	return currenthashlist


# implementation to read every file from disk to prevent ram from filling up. used for creating nogaps manifest.
def _compute_block_hashlist_fromdisk(offsetdict, blockcount, blocksize, hashalgorithm):
	"""private helper, used both the compute and check hashes"""

	print "[INFO] Calculating block hashes with algorithm", hashalgorithm, "..."

	if hashalgorithm in ['noop', 'none', None]:
		currenthashlist = ['']*blockcount
		return currenthashlist

	currenthashlist = []
	lastoffset = 0
	thisblock = ""
	pt = blockcount / 20
	nextprint = pt

	for blocknum in xrange(blockcount):

		if blockcount > 99 and blocknum >= nextprint:
			print blocknum, "/", blockcount,\
				  "("+str(int(round(blocknum*1.0/blockcount*100)))+"%) done..."
			nextprint = nextprint + pt

		while len(thisblock) < blocksize:

			if lastoffset in offsetdict:
				fd = open(offsetdict[lastoffset])
				print "[INFO] reading", offsetdict[lastoffset]

				thisfilecontents = fd.read()
				fd.close()
				lastlen = len(thisfilecontents)
				lastoffset = lastoffset + lastlen
				thisblock = thisblock + thisfilecontents

				del fd
				del thisfilecontents
			else:
				thisblock = thisblock + blocksize * "\0"

		# ... and check its hash
		currenthashlist.append(find_hash(thisblock[:blocksize], hashalgorithm))

		thisblock = thisblock[blocksize:]

	print "[INFO] All blocks done."
	return currenthashlist


def _validate_manifest(manifest):
	"""private function that validates the manifest is okay"""
	# it raises a TypeError if it's not valid for some reason
	if type(manifest) != dict:
		raise TypeError("Manifest must be a dict!")

	# check for the required keys
	for key in _required_manifest_keys:
		if key not in manifest:
			raise TypeError("Manifest must contain key: " + key + "!")

	# check specific things
	if len(manifest['blockhashlist']) != manifest['blockcount']:
		raise TypeError("There must be a hash for every manifest block")

	# otherwise, I guess I'll let this slide.   I don't want the checking to
	# be too version specific
	# JAC: Is this a dumb idea?   Should I just check it all?   Do I want
	# this to fail later?   Can the version be used as a proxy check for this?


_supported_hashalgorithms = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']

_supported_hashencodings = ['hex', 'raw']

def find_hash(contents, algorithm):
	"""Helper function for hashing"""

	# first, if it's a noop, do nothing. For testing and debugging only.
	if algorithm == 'noop' or algorithm == "none" or algorithm == None:
		return ''

	# accept things like: "sha1", "sha256-raw", etc.
	# before the '-' is one of the types known to hashlib.   After is

	# hashencoding = 'hex'
	if '-' in algorithm:
		# yes, this will raise an exception in some cases...
		hashalgorithmname, hashencoding = algorithm.split('-')

	# check the args
	if hashalgorithmname not in _supported_hashalgorithms:
		raise TypeError("Do not understand hash algorithm: '" + algorithm + "'")

	if hashencoding not in _supported_hashencodings:
		raise TypeError("Do not understand hash encoding: '" + algorithm + "'")

	if hashalgorithmname == 'sha256':
		hashobj = hashlib.sha256(contents)
	else:
		hashobj = hashlib.new(hashalgorithmname)
		hashobj.update(contents)

	if len(contents)>8:
		print "findhash dbg", len(contents), printhexstr(contents[0:4]), "...", printhexstr(contents[-4:]), hashobj.hexdigest()
	else:
		print "findhash dbg", len(contents), printhexstr(contents), hashobj.hexdigest()

	if hashencoding == 'raw':
		return hashobj.digest()
	elif hashencoding == 'hex':
		return hashobj.hexdigest()
	else:
		raise Exception("Internal Error! Unknown hashencoding '" + hashencoding + "'")


def transmit_mirrorinfo(mirrorinfo, vendorlocation, defaultvendorport=62293):
	"""
	<Purpose>
		Sends our mirror information to a vendor.

	<Arguments>
		vendorlocation: A string that contains the vendor location.   This can be of the form "IP:port", "hostname:port", "IP", or "hostname"

		defaultvendorport: the port to use if the vendorlocation does not include one.

	<Exceptions>
		TypeError if the args are the wrong types or malformed...

		various socket errors if the connection fails.

		ValueError if vendor does not accept the mirrorinfo

	<Side Effects>
		Contacts the vendor and retrieves data from it

	<Returns>
		None
	"""
	if type(mirrorinfo) != dict:
		raise TypeError("Mirror information must be a dictionary")

	# do the actual communication...
	answer = _remote_query_helper(vendorlocation, "MIRRORADVERTISE" + msgpack.packb(mirrorinfo), defaultvendorport)

	if answer != "OK":
		# JAC: I don't really like using ValueError. I should define a new one
		raise ValueError(answer)


def retrieve_rawmanifest(vendorlocation, defaultvendorport=62293):
	"""
	<Purpose>
		Retrieves the manifest data from a vendor.   It does not parse this
		data in any way.

	<Arguments>
		vendorlocation: A string that contains the vendor location.   This can be of the form "IP:port", "hostname:port", "IP", or "hostname"

		defaultvendorport: the port to use if the vendorlocation does not include one.

	<Exceptions>
		TypeError if the vendorlocation is the wrong type or malformed.

		various socket errors if the connection fails.

	<Side Effects>
		Contacts the vendor and retrieves data from it

	<Returns>
		A string containing the manifest data (unprocessed).   It is a good idea
		to use parse_manifest to ensure this data is correct.
	"""
	return _remote_query_helper(vendorlocation, "GET MANIFEST", defaultvendorport)

def retrieve_xorblock(socket, bitstring):

	"""
	<Purpose>
		Retrieves a block from a mirror.

	<Arguments>
		socket: an open socket to the mirror

		bitstring: a bit string that contains an appropriately sized request that specifies which blocks to combine.

	<Exceptions>
		TypeError if the arguments are the wrong types.  ValueError if the
		bitstring is the wrong size

		various socket errors if the connection fails.

	<Side Effects>
		Contacts the mirror and retrieves data from it

	<Returns>
		Binary data of size of 1 block. Several blocks XORed together.
	"""

	response = _remote_query_helper_sock(socket, "X" + bitstring)
	if response == 'Invalid request length':
		raise ValueError(response)

	return response

# only request a xorblock, without receiving it
def request_xorblock(socket, bitstring):
	session.sendmessage(socket, "X" + bitstring)


def retrieve_xorblock_chunked(socket, chunks):
	response = _remote_query_helper_sock(socket, "C" + msgpack.packb(chunks))

	if response == 'Invalid request length':
		raise ValueError(response)

	# print "CHUNKED Query", len(msgpack.packb(chunks)), "|", len(response)
	return response

# only request a xorblock, without receiving it
def request_xorblock_chunked(socket, chunks):
	session.sendmessage(socket, "C" + msgpack.packb(chunks))


def retrieve_xorblock_chunked_rng(socket, chunks):
	response = _remote_query_helper_sock(socket, "R" + msgpack.packb(chunks))
	if response == 'Invalid request length':
		raise ValueError(response)

	# print "CHUNKED RNG Query", len(msgpack.packb(chunks)), "|", len(response)
	return response

# only request a xorblock, without receiving it
def request_xorblock_chunked_rng(socket, chunks):
	session.sendmessage(socket, "R" + msgpack.packb(chunks))


def retrieve_xorblock_chunked_rng_parallel(socket, chunks):
	response = _remote_query_helper_sock(socket, "M" + msgpack.packb(chunks))
	if response == 'Invalid request length':
		raise ValueError(response)

	# print "CHUNKED RNG PAR Query", len(msgpack.packb(chunks)), "|", len(response)
	return response


# only request a xorblock, without receiving it
def request_xorblock_chunked_rng_parallel(socket, chunks):
	session.sendmessage(socket, "M" + msgpack.packb(chunks))


def retrieve_mirrorinfolist(vendorlocation, defaultvendorport=62293):
	"""
	<Purpose>
		Retrieves the mirrorinfolist from a vendor.

	<Arguments>
		vendorlocation: A string that contains the vendor location.   This can be
						of the form "IP:port", "hostname:port", "IP", or "hostname"

		defaultvendorport: the port to use if the vendorlocation does not include
											 one.

	<Exceptions>
		TypeError if the vendorlocation is the wrong type or malformed.

		various socket errors if the connection fails.

		SessionEOF or ValueError may be raised if the other end is not speaking the
		correct protocol

	<Side Effects>
		Contacts the vendor and retrieves data from it

	<Returns>
		A list of mirror information dictionaries.
	"""
	rawmirrordata = _remote_query_helper(vendorlocation, "GET MIRRORLIST", defaultvendorport)

	mirrorinfolist = msgpack.unpackb(rawmirrordata)

	# the mirrorinfolist must be a list (duh)
	if type(mirrorinfolist) != list:
		raise TypeError("Malformed mirror list from vendor. Is a " + str(type(mirrorinfolist)) + " not a list")

	for mirrorlocation in mirrorinfolist:
		# must be a string
		if type(mirrorlocation) != dict:
			raise TypeError("Malformed mirrorlocation from vendor. Is a " + str(type(mirrorlocation)) + " not a dict")

	# everything checked out
	return mirrorinfolist


# when a socket is already opened
def _remote_query_helper_sock(socket, command):
	# issue the relevant command
	session.sendmessage(socket, command)

	# receive and return the answer
	rawanswer = session.recvmessage(socket)
	return rawanswer

# opens a new socket each time...
def _remote_query_helper(serverlocation, command, defaultserverport):
	# private function that contains the guts of server communication.   It
	# issues a single query and then closes the connection.   This is used
	# both to talk to the vendor and also to talk to mirrors
	if type(serverlocation) != str and type(serverlocation) != unicode:
		raise TypeError("Server location must be a string, not " + str(type(serverlocation)))

	# now let's split it and ensure there are 0 or 1 colons
	splitlocationlist = serverlocation.split(':')

	if len(splitlocationlist) > 2:
		raise TypeError("Server location may not contain more than one colon")


	# now either set the port or use the default
	if len(splitlocationlist) == 2:
		serverport = int(splitlocationlist[1])
	else:
		serverport = defaultserverport

	# check that this port is in the right range
	if serverport <= 0 or serverport > 65535:
		raise TypeError("Server location's port is not in the allowed range")

	serverhostname = splitlocationlist[0]

	# now we actually download the information...

	# first open the socket
	serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serversocket.connect((serverhostname, serverport))

	# then issue the relevant command
	session.sendmessage(serversocket, command)

	# and return the answer
	rawanswer = session.recvmessage(serversocket)

	serversocket.close()

	return rawanswer


def parse_manifest(rawmanifestdata):
	"""
	<Purpose>
		Given raw manifest data, returns a dictionary containing a manifest
		dictionary.

	<Arguments>
		rawmanifestdata: a string containing the raw manifest data as is produced by the json module.

	<Exceptions>
		TypeError or ValueError if the manifest data is corrupt

	<Side Effects>
		None

	<Returns>
		A dictionary containing the manifest.
	"""

	if type(rawmanifestdata) != str:
		raise TypeError("Raw manifest data must be a string")

	manifestdict = msgpack.unpackb(rawmanifestdata)

	_validate_manifest(manifestdict)

	return manifestdict


def populate_xordatastore(manifestdict, xordatastore, datasource, dstype,
						  precompute):
	"""
	<Purpose>
		Adds the files listed in the manifestdict to the datastore

	<Arguments>
		manifestdict: a manifest dictionary.

		xordatastore: the XOR datastore that we should populate.

		datasource: The location to look for the files mentioned in the manifest

		dstype: The type (RAM, memory-mapped) of the datastore

		precompute: Specifies whether preprocessing should be performed

	<Exceptions>
		TypeError if the manifest is corrupt or the datasource is the wrong type.

		FileNotFound if the datasource does not contain a manifest file.

		IncorrectFileContents if the file listed in the manifest file has the wrong size or hash

	<Side Effects>
		None

	<Returns>
		None
	"""

	if type(manifestdict) != dict:
		raise TypeError("Manifest dict must be a dictionary")

	if type(datasource) != str and type(datasource) != unicode:
		raise TypeError("Mirror root must be a string")

	if dstype == "mmap":
		_mmap_database(xordatastore, datasource)
	else: # RAM
		_add_data_to_datastore(xordatastore, manifestdict['fileinfolist'], datasource, manifestdict['hashalgorithm'], manifestdict['datastore_layout'], manifestdict['blocksize'])


	hashlist = _compute_block_hashlist_fromdatastore(xordatastore, manifestdict['blockcount'], manifestdict['blocksize'], manifestdict['hashalgorithm'])

	for blocknum in range(manifestdict['blockcount']):

		if hashlist[blocknum] != manifestdict['blockhashlist'][blocknum]:
			raise TypeError("Despite matching file hashes, block '" + str(blocknum) + "' has an invalid hash.\nCorrupt manifest or dirty xordatastore")
	# We're done!

	if precompute:
		print("Preprocessing data...")
		start = time.clock()
		xordatastore.finalize()
		elapsed = (time.clock() - start)
		print "Preprocessing done. Took %f seconds." % elapsed


def _mmap_database(xordatastore, dbname):
	xordatastore.initialize(dbname)


def _add_data_to_datastore(xordatastore, fileinfolist, rootdir, hashalgorithm, datastore_layout, blocksize):
	# Private helper to populate the datastore
	if not datastore_layout in ['nogaps', 'eqdist']:
		raise ValueError("Unknown datastore layout: "+datastore_layout)

	# go through the files one at a time and populate the xordatastore
	for thisfiledict in fileinfolist:

		thisrelativefilename = thisfiledict['filename']
		thisfilehash = thisfiledict['hash']
		thisfilelength = thisfiledict['length']

		thisfilename = os.path.join(rootdir, thisrelativefilename)

		# read in the files and populate the xordatastore
		if not os.path.exists(thisfilename):
			raise FileNotFound("File '" + thisrelativefilename + "' listed in manifest cannot be found in manifest root: '" + rootdir + "'.")

		# can't go above the root!
		# JAC: I would use relpath, but it's 2.6 and on
		if not os.path.normpath(os.path.abspath(thisfilename)).startswith(os.path.abspath(rootdir)):
			raise TypeError("File in manifest cannot go back from the root dir!!!")

		# get the relevant data
		thisfilecontents = open(thisfilename).read()

		# let's see if this has the right size
		if len(thisfilecontents) != thisfilelength:
			raise IncorrectFileContents("File '" + thisrelativefilename + "' has the wrong size")

		# let's see if this has the right hash
		if thisfilehash != find_hash(thisfilecontents, hashalgorithm):
			raise IncorrectFileContents("File '" + thisrelativefilename + "' has the wrong hash")

		# and add it to the datastore
		if datastore_layout == 'nogaps':
			thisoffset = thisfiledict['offset']
			xordatastore.set_data(thisoffset, thisfilecontents)
		elif datastore_layout == 'eqdist':
			offsets = thisfiledict['offsets']
			offsetsoffset = 0
			fileoffset = 0
			while fileoffset < len(thisfilecontents):
				block_remaining_bytes = blocksize - (offsets[offsetsoffset]%blocksize)
				bytes_to_add = min(len(thisfilecontents)-fileoffset, block_remaining_bytes)

				xordatastore.set_data(
					offsets[offsetsoffset], thisfilecontents[fileoffset:fileoffset+bytes_to_add])

				fileoffset += bytes_to_add
				offsetsoffset += 1


def _create_offset_dict(offsetdict, fileinfolist, rootdir, hashalgorithm):
	# Private helper to populate the datastore

	# go through the files one at a time and populate the xordatastore
	for thisfiledict in fileinfolist:

		thisrelativefilename = thisfiledict['filename']
		thisfilehash = thisfiledict['hash']
		thisoffset = thisfiledict['offset']
		thisfilelength = thisfiledict['length']

		thisfilename = os.path.join(rootdir, thisrelativefilename)

		# read in the files and populate the xordatastore
		if not os.path.exists(thisfilename):
			raise FileNotFound("File " + thisrelativefilename + " -->" + thisfilename + " listed in manifest cannot be found in manifest root: " + rootdir + ".")

		# can't go above the root!
		# JAC: I would use relpath, but it's 2.6 and on
		if not os.path.normpath(os.path.abspath(thisfilename)).startswith(os.path.abspath(rootdir)):
			raise TypeError("File in manifest cannot go back from the root dir!!!")

		# get the relevant data
		fd = open(thisfilename)
		thisfilecontents = fd.read()

		# let's see if this has the right size
		if len(thisfilecontents) != thisfilelength:
			raise IncorrectFileContents("File '" + thisrelativefilename + "' has the wrong size")

		# let's see if this has the right hash
		if thisfilehash != find_hash(thisfilecontents, hashalgorithm):
			raise IncorrectFileContents("File '" + thisrelativefilename + "' has the wrong hash")

		fd.close()
		del fd
		del thisfilecontents

		# and add it to the dict
		offsetdict[thisoffset] = thisfilename

	print "[INFO] Offset-Dict generated."


def datastore_layout_function_nogaps(fileinfolist, rootdir, blocksize, hashalgorithm):
	"""
	<Purpose>
		Specifies how to map a set of files into offsets in an xordatastore.
		This simple function just adds them linearly.

	<Arguments>
		fileinfolist: a list of dictionaries with file information

		rootdir: the root directory where the files live

		block_size: The size of a block of data.

	<Exceptions>
		TypeError, IndexError, or KeyError if the arguements are incorrect

	<Side Effects>
		Modifies the fileinfolist to add offset elements to each dict

	<Returns>
		None
	"""

	print "[INFO] Using `nogaps` algorithm."

	# Note, this algorithm doesn't use the blocksize.   Most of algorithms will.
	# We also don't use the rootdir.   I think this is typical

	currentoffset = 0

	for thisfileinfo in fileinfolist:
		thisfileinfo['offset'] = currentoffset
		currentoffset = currentoffset + thisfileinfo['length']

	blockcount = int(math.ceil(currentoffset * 1.0 / blocksize))

	# let's ensure the offsets are valid...
	# build a list of tuples with offset, etc. info...
	offsetlengthtuplelist = []
	for fileinfo in fileinfolist:
		offsetlengthtuplelist.append((fileinfo['offset'], fileinfo['length']))

	# ...sort the tuples so that it's easy to walk down them and check for
	# overlapping entries...
	offsetlengthtuplelist.sort()

	# ...now, we need to ensure the values don't overlap.
	nextfreeoffset = 0
	for offset, length in offsetlengthtuplelist:
		if offset < 0:
			raise TypeError("Offset generation led to negative offset!")
		if length < 0:
			raise TypeError("File lengths must be positive!")

		if nextfreeoffset > offset:
			raise TypeError("Error! Offset generation led to overlapping files!")

		# since this list is sorted by offset, this should ensure the property we want is upheld.
		nextfreeoffset = offset + length

	offsetdict = {}
	_create_offset_dict(offsetdict, fileinfolist, rootdir, hashalgorithm)
	print "[INFO] Indexing done ..."

	# and it is time to get the blockhashlist...
	# manifestdict['blockhashlist'] = _compute_block_hashlist(offsetdict, manifestdict['blockcount'], manifestdict['blocksize'], manifestdict['hashalgorithm'])
	blockhashlist = _compute_block_hashlist_fromdisk(offsetdict, blockcount, blocksize, hashalgorithm)

	return blockhashlist


def datastore_layout_function_eqdist(fileinfolist, rootdir, blocksize, hashalgorithm):
	"""
	<Purpose>
		Specifies how to map a set of files into offsets in an xordatastore.
		This function distributes them equally over the database.

	<Arguments>
		fileinfolist: a list of dictionaries with file information

		rootdir: the root directory where the files live

		block_size: The size of a block of data.

	<Exceptions>
		TypeError, IndexError, or KeyError if the arguements are incorrect

	<Side Effects>
		Modifies the fileinfolist to add offset elements to each dict

	<Returns>
		None
	"""

	print "[INFO] Using `eqdist` algorithm."

	# Note, this algorithm doesn't use the blocksize.   Most of algorithms will.
	# We also don't use the rootdir.   I think this is typical

	db_length = 0
	for thisfileinfo in fileinfolist:
		db_length = db_length + thisfileinfo['length']

	blockcount = int(math.ceil(db_length * 1.0 / blocksize))

	free_blocks = range(1, blockcount)
	currentoffset = 0
	currentblock = 0
	last_block = -1

	# progress counter
	hashedblocks = 0
	pt = blockcount*1.0/20
	nextprint = pt

	# define the hashlist for the block hashes
	hashlist = ['']*blockcount
	current_block_content = ""


	for thisfileinfo in fileinfolist:
		thisfileinfo['offsets'] = []

		thisfilename = os.path.join(rootdir, thisfileinfo['filename'])
		print "[INFO] reading", thisfilename

		# prevent access above rootdir
		if not os.path.normpath(os.path.abspath(thisfilename)).startswith(os.path.abspath(rootdir)):
			raise TypeError("File in manifest cannot go back from the root dir!!!")

		# open the file for reading (to compute the hash for the current block)
		fd = open(thisfilename)

		remainingbytes = thisfileinfo['length']
		blocks_per_file = thisfileinfo['length']*1.0 / blocksize
		block_steps = max(2, int(blockcount/blocks_per_file))
		current_step = 0

		while remainingbytes > 0:
			block_remaining_bytes = (blocksize - (currentoffset % blocksize))
			thisfileinfo['offsets'].append(currentoffset)

			bytes_to_add = min(remainingbytes, block_remaining_bytes)
			remainingbytes -= bytes_to_add
			currentoffset += bytes_to_add
			current_block_content += fd.read(bytes_to_add)

			if currentoffset % blocksize == 0 and len(free_blocks) != 0:
				# block is full
				last_block = currentoffset/blocksize - 1

				# show progress
				hashedblocks += 1
				if blockcount > 99 and hashedblocks >= nextprint:
					print hashedblocks, "/", blockcount,\
						  "("+str(int(round(hashedblocks*1.0/blockcount*100)))+"%) done..."
					nextprint = nextprint + pt


				# calculate hash for block
				hashlist[last_block] = find_hash(current_block_content, hashalgorithm)
				current_block_content = ""

				# find new free block
				current_step += 1
				block_candidate = (last_block + block_steps) % blockcount

				while block_candidate not in free_blocks:
					block_candidate += 1
					if block_candidate == blockcount:
						block_candidate = 0

				free_blocks.remove(block_candidate)

				currentoffset = block_candidate * blocksize
				block_remaining_bytes = blocksize

		# close the file descriptor
		fd.close()
		del fd


	assert len(free_blocks) == 0

	# the last block has to be padded to full block size
	block_remaining_bytes = (blocksize - (currentoffset % blocksize))
	current_block_content += block_remaining_bytes * "\0"

	# calculate the hash for the last block
	current_block = currentoffset/blocksize
	hashlist[current_block] = find_hash(current_block_content, hashalgorithm)

	for h in hashlist:
		assert h != ''

	#currentoffset = 0
	#for thisfileinfo in fileinfolist:
	#	thisfileinfo['offset'] = currentoffset
	#	currentoffset = currentoffset + thisfileinfo['length']

	return hashlist


def _find_blockloc_from_offset(offset, sizeofblocks):
	# Private helper function that translates an offset into (block, offset)
	assert offset >= 0

	return (offset / sizeofblocks, offset % sizeofblocks)


def extract_file_from_blockdict(filename, manifestdict, blockdict):
	"""
	<Purpose>
		Reconstitutes a file from a block dict

	<Arguments>
		filename: the file within the release we are asking about

		manifestdict: the manifest for the release

		blockdict: a dictionary of blocknum -> blockcontents

	<Exceptions>
		TypeError, IndexError, or KeyError if the args are incorrect

	<Side Effects>
		None

	<Returns>
		A string containing the file contents
	"""

	blocksize = manifestdict['blocksize']
	database_layout = manifestdict['datastore_layout']

	for fileinfo in manifestdict['fileinfolist']:
		if filename == fileinfo['filename']:

			if database_layout == 'nogaps':
				offset = fileinfo['offset']
				quantity = fileinfo['length']

				# Let's get the block information
				(startblock, startoffset) = _find_blockloc_from_offset(offset, blocksize)
				(endblock, endoffset) = _find_blockloc_from_offset(offset + quantity, blocksize)

				# Case 1: this does not cross blocks
				if startblock == endblock:
					return blockdict[startblock][startoffset:endoffset]

				# Case 2: this crosses blocks

				# we'll build up the string starting with the first block...
				currentstring = blockdict[startblock][startoffset:]

				# now add in the 'middle' blocks.   This is all of the blocks
				# after the start and before the end
				for currentblock in range(startblock + 1, endblock):
					currentstring += blockdict[currentblock]

				# this check is needed because we might be past the last block.
				if endoffset > 0:
					# finally, add the end block.
					currentstring += blockdict[endblock][:endoffset]

				# and return the result
				return currentstring
			elif database_layout == 'eqdist':
				offsets = fileinfo['offsets']
				quantity = fileinfo['length']

				currentstring = ''

				for offset in offsets:
					(block, blockoffset) = _find_blockloc_from_offset(offset, blocksize)
					currentstring += blockdict[block][blockoffset:]

				currentstring = currentstring[:quantity]
				return currentstring
			else:
				raise Exception("Unknown database layout")

def get_blocklist_for_file(filename, manifestdict):
	"""
	<Purpose>
		Get the list of blocks needed to reconstruct a file

	<Arguments>
		filename: the file within the release we are asking about

		manifestdict: the manifest for the release

	<Exceptions>
		TypeError, IndexError, or KeyError if the manifestdict / filename are
		corrupt

	<Side Effects>
		None

	<Returns>
		A list of blocks numbers
	"""

	blocksize = manifestdict['blocksize']

	for fileinfo in manifestdict['fileinfolist']:
		if filename == fileinfo['filename']:
			if manifestdict['datastore_layout'] == 'nogaps':
				# it's the starting offset / blocksize until the
				# ending offset -1 divided by the blocksize
				# I do + 1 because range will otherwise omit the last block
				return range(fileinfo['offset'] / blocksize, (fileinfo['offset'] + fileinfo['length'] - 1) / blocksize + 1)
			elif manifestdict['datastore_layout'] == 'eqdist':
				offsets = fileinfo['offsets']
				blocks = []
				for offset in offsets:
					blocks.append(offset/blocksize)
				return blocks
			else:
				raise Exception("Unknown datastore layout")

	raise TypeError("File is not in manifest")


def get_filenames_in_release(manifestdict):
	"""
	<Purpose>
		Get the list of files in a manifest

	<Arguments>
		manifestdict: the manifest for the release

	<Exceptions>
		TypeError, IndexError, or KeyError if the manifestdict is corrupt

	<Side Effects>
		None

	<Returns>
		A list of file names
	"""

	filenamelist = []

	for fileinfo in manifestdict['fileinfolist']:
		filenamelist.append(fileinfo['filename'])

	return filenamelist


def _generate_fileinfolist(startdirectory, hashalgorithm="sha256-raw"):
	"""private helper.   Generates a list of file information dictionaries for all files under startdirectory."""

	fileinfo_list = []

	# let's walk through the directories and add the files + sizes
	for parentdir, junkchilddirectories, filelist in os.walk(startdirectory):
		for filename in filelist:
			thisfiledict = {}

			# we want the relative name in the manifest, not the actual path / name
			thisfiledict['filename'] = filename
			fullfilename = os.path.join(parentdir, filename)

			thisfiledict['length'] = os.path.getsize(fullfilename)

			# get the hash
			fd = open(fullfilename)
			filecontents = fd.read()
			thisfiledict['hash'] = find_hash(filecontents, hashalgorithm)

			fd.close()
			del filecontents
			del fd

			fileinfo_list.append(thisfiledict)

	print "[INFO] Fileinfolist generation done."
	return fileinfo_list


def _write_db(startdirectory, dbname):
	"""private helper. Writes all files into a single db file"""

	oo = open(dbname, 'w')

	# Header
	oo.write("RAIDPIRDB_v0.9.3")

	# let's walk through the directories and add the files + sizes
	for parentdir, junkchilddirectories, filelist in os.walk(startdirectory):
		for filename in filelist:
			thisfiledict = {}
			# we want the relative name in the manifest, not the actual path / name
			fullfilename = os.path.join(parentdir, filename)

			# open and read file
			fd = open(fullfilename)
			filecontents = fd.read()

			#append it to single db file
			oo.write(filecontents);

			fd.close()
			del filecontents
			del fd

	print "Database", dbname, "created."
	oo.close();


def bits_to_bytes(num_bits):
	"""compute bitstring length in bytes from number of blocks"""
	return (num_bits + 7) >> 3


def set_bitstring_bit(bitstring, bitnum, valuetoset):
	"""set a bit in a bitstring, 0 = MSB"""
	bytepos = bitnum >> 3
	bitpos = 7 - (bitnum % 8)

	bytevalue = ord(bitstring[bytepos])
	# if setting to 1...
	if valuetoset:
		if bytevalue & (2 ** bitpos):
			# nothing to do, it's set.
			return bitstring
		else:
			return bitstring[:bytepos] + chr(bytevalue + (2 ** bitpos)) + bitstring[bytepos + 1:]

	else:  # I'm setting it to 0...

		if bytevalue & (2 ** bitpos):
			return bitstring[:bytepos] + chr(bytevalue - (2 ** bitpos)) + bitstring[bytepos + 1:]
		else:
			# nothing to do, it's not set.
			return bitstring


def get_bitstring_bit(bitstring, bitnum):
	"""returns a single bit from within a bitstring, 0 = MSB"""
	bytepos = bitnum >> 3
	bitpos = 7 - (bitnum % 8)

	# we want to return 0 or 1.   I'll AND 2^bitpos and then divide by it
	return (ord(bitstring[bytepos]) & (2 ** bitpos)) >> bitpos


def flip_bitstring_bit(bitstring, bitnum):
	"""reverses the setting of a bit, 0 = MSB"""
	targetbit = get_bitstring_bit(bitstring, bitnum)

	# 0 -> 1, 1 -> 0
	targetbit = 1 - targetbit

	return set_bitstring_bit(bitstring, bitnum, targetbit)


def flip_array_bit(ba, bitnum):
	"""flips a bit in an array, 0 = MSB. Works with numpy arrays or byte arrays"""
	ba[bitnum >> 3] ^= (1 << (7 - (bitnum % 8)))
	return ba


def create_manifest(rootdir=".", hashalgorithm="sha256-raw", block_size=1024 * 1024, datastore_layout="nogaps", vendorhostname=None, vendorport=62293):
	"""
	<Purpose>
		Create a manifest

	<Arguments>
		rootdir: The area to walk looking for files to add to the manifest

		hashalgorithm: The hash algorithm to use to validate file contents

		block_size: The size of a block of data.

		datastore_layout: specifies how to lay out the files in blocks.

	<Exceptions>
		TypeError if the arguments are corrupt or of the wrong type

		FileNotFound if the rootdir does not contain a manifest file.

		IncorrectFileContents if the file listed in the manifest file has the wrong size or hash

	<Side Effects>
		This function creates an XORdatastore while processing.   This may use
		a very large amount of memory.   (This is not fundamental, and is done only
		for convenience).

	<Returns>
		The manifest dictionary
	"""

	if vendorhostname == None:
		raise TypeError("Must specify vendor server name")

	if ':' in vendorhostname:
		raise TypeError("Vendor server name must not contain ':'")

	# general workflow:
	#   set the global parameters
	#   build an xordatastore and add file information as you go
	#   derive hash information from the xordatastore

	manifestdict = {}

	manifestdict['manifestversion'] = "2.0"
	manifestdict['hashalgorithm'] = hashalgorithm
	manifestdict['blocksize'] = block_size
	manifestdict['vendorhostname'] = vendorhostname
	manifestdict['vendorport'] = vendorport
	manifestdict['datastore_layout'] = datastore_layout

	# first get the file information
	fileinfolist = _generate_fileinfolist(rootdir, manifestdict['hashalgorithm'])

	# Let's see how many blocks we need
	db_length = 0
	for fileinfo in fileinfolist:
		db_length += fileinfo['length']

	manifestdict['blockcount'] = int(math.ceil(db_length * 1.0 / manifestdict['blocksize']))

	# now let's assign the files to offsets as the caller requests and create
	# the hashes for the PIR blocks
	if datastore_layout == "nogaps":
		manifestdict['blockhashlist'] = datastore_layout_function_nogaps(fileinfolist, rootdir, manifestdict['blocksize'], hashalgorithm)
	elif datastore_layout == "eqdist":
		manifestdict['blockhashlist'] = datastore_layout_function_eqdist(fileinfolist, rootdir, manifestdict['blocksize'], hashalgorithm)
	else:
		print "Unknown datastore layout function. Try 'nogaps' or 'eqdist'"
		sys.exit(1)

	manifestdict['fileinfolist'] = fileinfolist

	# we are done!
	return manifestdict


def randombits(bitlength):
	"""
	<Purpose>
		Creates a random string with the supplied bitlength (the rightmost bits are zero if bitlength is not a multiple of 8)

	<Arguments>
		bitlength: the length of the randomness in bits (not Bytes)

	<Returns>
		A random byte-string of supplied bitlength
	"""
	assert bitlength != 0, "[randombits] bitlength should not be zero"
	randombytes = os.urandom(bits_to_bytes(bitlength) - 1)

	# additional_bits is the number of bits for the last byte
	additional_bits = bitlength % 8
	if bitlength % 8 == 0: additional_bits = 8

	# let's add the additional_bits to the randombytes array
	randombytes += chr(ord(os.urandom(1)) & ((0xff00 >> additional_bits) & 255))
	return randombytes


def build_bitstring_from_chunks(chunks, k, chunklen, lastchunklen):
	"""
	<Purpose>
		Creates a single bitstring from given chunks

	<Arguments>
		chunks: the dictionary of chunk indices and query data (random strings)

		k: the number of servers

		chunklen: the length of the first k-1 chunks in Bytes

		lastchunklen: the length of the last chunk in Bytes ( >= chunklen )

	<Returns>
		A random byte-string of supplied bitlength
	"""

	result = ""
	chunklen = chunklen / 8
	lastchunklen = bits_to_bytes(lastchunklen)

	for i in range(0, k):
		if i in chunks:
			result = result + chunks[i]
		else:
			if i < k - 1:
				result = result + chunklen * "\0"
			else:
				result = result + lastchunklen * "\0"

	return result


def build_bitstring_from_chunks_parallel(chunks, k, chunklen, lastchunklen):
	"""
	<Purpose>
		Creates a single bitstring from given chunks

	<Arguments>
		chunks: the dictionary of chunk indices and query data (random strings)

		k: the number of servers

		chunklen: the length of the first k-1 chunks in Bytes

		lastchunklen: the length of the last chunk in Bytes ( >= chunklen )

	<Returns>
		A dictionary
	"""

	result = {}
	chunklen = chunklen / 8
	lastchunklen = bits_to_bytes(lastchunklen)


	for c in chunks:
		bitstring = ""

		for i in xrange(k):

			if i == c:
				bitstring = bitstring + chunks[c]
			else:
				if i < k - 1:
					bitstring = bitstring + chunklen * "\0"
				else:
					bitstring = bitstring + lastchunklen * "\0"

		result[c] = bitstring

	return result


def initAES(seed):
	"""
	<Purpose>
		initializes the AES cipher and resets the counter

	<Arguments>
		seed: the aes key

	<Returns>
		an initialized cipher object
	"""

	ctr = Counter.new(128)
	return AES.new(seed, AES.MODE_CTR, counter=ctr)


def nextrandombitsAES(cipher, bitlength):
	"""
	<Purpose>
		generate random bits using AES-CTR

	<Arguments>
		bitlength: the lenght of the random string in BITS

	<Side Effects>
		Increases the AES counter

	<Returns>
		A random string with the supplied bitlength (the rightmost bits are zero if bitlength is not a multiple of 8)
	"""

	# offset for the last byte
	bytelength = bits_to_bytes(bitlength)
	bitoffset = bitlength % 8

	if bitoffset > 0:
		# if the bitlength is not a multiple of 8, clear the rightmost bits
		pt = (bytelength - 1) * "\0"

		randombytes = cipher.encrypt(pt)
		b = ord(cipher.encrypt("\0"))
		for i in range(8 - bitoffset):
			b &= ~(1 << i)
		b = chr(b)
		randombytes += b
		return randombytes
	else:
		pt = bytelength * "\0"
		return cipher.encrypt(pt)

def printhexstr(s):
	return ''.join(x.encode('hex') for x in s)
