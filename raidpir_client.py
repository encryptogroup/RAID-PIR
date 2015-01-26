"""
<Author>
	Daniel Demmler
	(inspired from upPIR by Justin Cappos)
	(inspired from a previous version by Geremy Condra)

<Date>
	December 2014

<Description>
	Client code for retrieving RAID-PIR files. This program uses a manifest
	to communicate with a vendor and retrieve a list of mirrors.   The client
	then _privately_ downloads the appropriate files from mirrors in the mirror
	list.  None of the mirrors can tell what file or files were downloaded.

	For more technical explanation, please see the paper.

<Usage>
	see python raidpir_client.py --help

	$ python raidpir_client.py [--retrievemanifestfrom <IP>:<PORT>] [-r <REDUNDANCY>] [-R] [-p] [--vendorip <IP>] file1 [file2 ...]


<Options>
	See below
"""

# This file is laid out in two main parts.   First, there are some helper
# functions to do moderately complex things like retrieving a block from a
# mirror or split a file into blocks.   The second part contains the option
# parsing and main.   To get an overall feel for the code, it is recommended
# to follow the execution from main on.
#
# EXTENSION POINTS:
#
# Making the client extensible is a major problem.   In particular, we will
# need to modify mirror selection, block selection, malicious mirror detection,
# and avoiding slow nodes simultaneously.   To do this effectively, we need
# some sort of mechanism that gives the programmer control over how to handle
# these.
#
# The XORRequestor interface is used to address these issues.   A programmer
# The programmer defines an object that is provided the manifest,
# mirrorlist, and blocks to retrieve.   The XORRequestor object must support
# several methods: get_next_xorrequest(), notify_failure(xorrequest),
# notify_success(xorrequest, xordata), and return_block(blocknum).   The
# request_blocks_from_mirrors function in this file will use threads to call
# these methods to determine what to retrieve.   The notify_* routines are
# used to inform the XORRequestor object of prior results so that it can
# decide how to issue future block requests.   This separates out the 'what'
# from the 'how' but has a slight loss of control.  Note that the block
# reconstruction, etc. is done here to allow easy extensibility of malicious
# mirror detection / vendor notification.
#
#
# The manifest file could also be extended to support huge files (those that
# span multiple releases).   The client would need to download files from
# multiple releases and then stitch them back together.   This would require
# minor changes (or possibly could be done using this code as a black box).
#

import sys

import optparse

# helper functions that are shared
import raidpirlib

# used to issue requests in parallel
import threading

# I really should have a way to do this based upon command line options
import simplexorrequestor

# for basename
import os.path


def _request_helper(rxgobj, tid):
	# Private helper to get requests. Multiple threads will execute this, each with a unique tid.
	thisrequest = rxgobj.get_next_xorrequest(tid)
	socket = thisrequest[0]['socket'] #the socket is fixed for each thread, so we only need to do this once

	# go until there are no more requests
	while thisrequest != ():
		bitstring = thisrequest[2]
		try:
			# request the XOR block...
			raidpirlib.request_xorblock_from_mirror(socket, bitstring)

		except Exception, e:
			if 'socked' in str(e):
				rxgobj.notify_failure(thisrequest)
				sys.stdout.write('F')
				sys.stdout.flush()
			else:
				# otherwise, re-raise...
				raise

		# regardless of failure or success, get another request...
		thisrequest = rxgobj.get_next_xorrequest(tid)

	# and that's it!
	return

def _request_helper_chunked(rxgobj, tid):
	# Private helper to get requests. Potentially multiple threads will execute this, each with a unique tid.
	thisrequest = rxgobj.get_next_xorrequest(tid)
	socket = thisrequest[0]['socket'] #the socket is fixed for each thread, so we only need to do this once
	rqtype = thisrequest[3] #the request type is also fixed

	# go until there are no more requests
	while thisrequest != ():
		chunks = thisrequest[2]

		try:
			# request the XOR block...
			if rqtype == 1: # chunks and seed expansion
				raidpirlib.request_xorblock_from_mirror_chunked_rng(socket, chunks)

			elif rqtype == 2: # chunks, seed expansion and parallel
				raidpirlib.request_xorblock_from_mirror_chunked_rng_parallel(socket, chunks)

			else: # only chunks (redundancy)
				raidpirlib.request_xorblock_from_mirror_chunked(socket, chunks)

		except Exception, e:
			if 'socked' in str(e):
				rxgobj.notify_failure(thisrequest)
				sys.stdout.write('F')
				sys.stdout.flush()
			else:
				# otherwise, re-raise...
				raise

		#else:
			# we retrieved it successfully...
			#rxgobj.notify_success(thisrequest, xorblock)
			# sys.stdout.write('.')
			# sys.stdout.flush()

		# regardless of failure or success, get another request...
		thisrequest = rxgobj.get_next_xorrequest(tid)

	# and that's it!
	return


def request_blocks_from_mirrors(requestedblocklist, manifestdict, redundancy, rng, parallel):

	"""
	<Purpose>
		Retrieves blocks from mirrors

	<Arguments>
		requestedblocklist: the blocks to acquire

		manifestdict: the manifest with information about the release

	<Side Effects>
		Contacts mirrors to retrieve blocks.    It uses some global options

	<Exceptions>
		TypeError may be raised if the provided lists are invalid.
		socket errors may be raised if communications fail.

	<Returns>
		A dict mapping blocknumber -> blockcontents.
	"""

	# let's get the list of mirrors...
	if _commandlineoptions.vendorip == None:
		mirrorinfolist = raidpirlib.retrieve_mirrorinfolist(manifestdict['vendorhostname'], manifestdict['vendorport'])
	else:
		mirrorinfolist = raidpirlib.retrieve_mirrorinfolist(_commandlineoptions.vendorip)

	print "Mirrors: ", mirrorinfolist

	# no chunks (regular upPIR / Chor)
	if redundancy == None:

		# let's set up a requestor object...
		rxgobj = simplexorrequestor.RandomXORRequestor(mirrorinfolist, requestedblocklist, manifestdict, _commandlineoptions.numberofmirrors)

		print "Blocks to request:", len(rxgobj.activemirrorinfolist[0]['blockbitstringlist'])

		# let's fire up the requested number of threads.   Our thread will also participate (-1 because of us!)
		for tid in xrange(_commandlineoptions.numberofmirrors - 1):
			threading.Thread(target=_request_helper, args=[rxgobj, tid]).start()

		_request_helper(rxgobj, _commandlineoptions.numberofmirrors - 1)

		# wait for receiving threads to finish
		for mirror in rxgobj.activemirrorinfolist:
			mirror['rt'].join()

		rxgobj.cleanup()

	else: # chunks

		# let's set up a chunk requestor object...
		rxgobj = simplexorrequestor.RandomXORRequestorChunks(mirrorinfolist, requestedblocklist, manifestdict, _commandlineoptions.numberofmirrors, redundancy, rng, parallel)

		print "# Blocks needed:", len(rxgobj.activemirrorinfolist[0]['blocksneeded'])

		if parallel:
			print "# Requests:", len(rxgobj.activemirrorinfolist[0]['blockchunklist'])

		#chunk lengths in BYTE
		global chunklen
		global lastchunklen
		chunklen = manifestdict['blockcount'] / 8 / _commandlineoptions.numberofmirrors
		lastchunklen = raidpirlib.compute_bitstring_length(manifestdict['blockcount']) - (_commandlineoptions.numberofmirrors-1)*chunklen

		# let's fire up the requested number of threads.   Our thread will also participate (-1 because of us!)
		for tid in xrange(_commandlineoptions.numberofmirrors - 1):
			threading.Thread(target=_request_helper_chunked, args=[rxgobj, tid]).start()

		_request_helper_chunked(rxgobj, _commandlineoptions.numberofmirrors - 1)

		# wait for receiving threads to finish
		for mirror in rxgobj.activemirrorinfolist:
			mirror['rt'].join()

		rxgobj.cleanup()


	# okay, now we have them all. Let's get the returned dict ready.
	retdict = {}
	for blocknum in requestedblocklist:
		retdict[blocknum] = rxgobj.return_block(blocknum)

	return retdict


def request_files_from_mirrors(requestedfilelist, redundancy, rng, parallel, manifestdict):
	"""
	<Purpose>
		Reconstitutes files by privately contacting mirrors

	<Arguments>
		requestedfilelist: the files to acquire
		redundancy: use chunks and overlap this often
		rng: use rnd to generate latter chunks
		parallel: query one block per chunk
		manifestdict: the manifest with information about the release

	<Side Effects>
		Contacts mirrors to retrieve files. They are written to disk

	<Exceptions>
		TypeError may be raised if the provided lists are invalid.
		socket errors may be raised if communications fail.

	<Returns>
		None
	"""

	neededblocks = []
	#print "Request Files:"
	# let's figure out what blocks we need
	for filename in requestedfilelist:
		theseblocks = raidpirlib.get_blocklist_for_file(filename, manifestdict)
		#print filename, theseblocks

		# add the blocks we don't already know we need to request
		for blocknum in theseblocks:
			if blocknum not in neededblocks:
				neededblocks.append(blocknum)

	# do the actual retrieval work
	blockdict = request_blocks_from_mirrors(neededblocks, manifestdict, redundancy, rng, parallel)


	# now we should write out the files
	for filename in requestedfilelist:
		filedata = raidpirlib.extract_file_from_blockdict(filename, manifestdict, blockdict)

		# let's check the hash
		thisfilehash = raidpirlib.find_hash(filedata, manifestdict['hashalgorithm'])

		for fileinfo in manifestdict['fileinfolist']:
			# find this entry
			if fileinfo['filename'] == filename:
				if thisfilehash == fileinfo['hash']:
					# we found it and it checks out!
					break
				else:
					raise Exception("Corrupt manifest has incorrect file hash despite passing block hash checks")
		else:
			raise Exception("Internal Error: Cannot locate fileinfo in manifest")


		# open the filename w/o the dir and write it
		filenamewithoutpath = os.path.basename(filename)
		open(filenamewithoutpath, "w").write(filedata)
		print "wrote", filenamewithoutpath


########################## Option parsing and main ###########################
_commandlineoptions = None

def parse_options():
	"""
	<Purpose>
		Parses command line arguments.

	<Arguments>
		None

	<Side Effects>
		All relevant data is added to _commandlineoptions

	<Exceptions>
		These are handled by optparse internally.   I believe it will print / exit
		itself without raising exceptions further.   I do print an error and
		exit if there are extra args...

	<Returns>
		The list of files to retrieve
	"""
	global _commandlineoptions

	# should be true unless we're initing twice...
	assert _commandlineoptions == None

	parser = optparse.OptionParser()

	parser.add_option("", "--retrievemanifestfrom", dest="retrievemanifestfrom",
				type="string", metavar="vendorIP:port", default="",
				help="Specifies the vendor to retrieve the manifest from (default None).")

	parser.add_option("", "--printfilenames", dest="printfiles",
				action="store_true", default=False,
				help="Print a list of all available files in the manifest file.")

	parser.add_option("", "--vendorip", dest="vendorip", type="string", metavar="IP",
				default=None, help="Vendor IP for overwriting the value from manifest; for testing purposes.")

	parser.add_option("-m", "--manifestfile", dest="manifestfilename",
				type="string", default="manifest.dat",
				help="The manifest file to use (default manifest.dat).")

	parser.add_option("-k", "--numberofmirrors", dest="numberofmirrors",
				type="int", default=2,
				help="How many servers do we query? (default 2)")

	parser.add_option("-r", "--redundancy", dest="redundancy",
				type="int", default=None,
				help="Activates chunks and specifies redundancy (how often they overlap). (default None)")

	parser.add_option("-R", "--rng", action="store_true", dest="rng", default=False,
				help="Use seed expansion from RNG for latter chunks (default False). Requires -r")

	parser.add_option("-p", "--parallel", action="store_true", dest="parallel", default=False,
				help="Query one block per chunk in parallel (default False). Requires -r")

	# let's parse the args
	(_commandlineoptions, remainingargs) = parser.parse_args()

	# k>=2
	if _commandlineoptions.numberofmirrors < 2:
		print "Mirrors to contact must be > 1"
		sys.exit(1)

	# r >= 2
	if _commandlineoptions.redundancy != None and _commandlineoptions.redundancy < 2:
		print "Redundancy must be > 1"
		sys.exit(1)

	# r <= k
	if _commandlineoptions.redundancy > _commandlineoptions.numberofmirrors:
		print "Redundancy must be less or equal to number of mirrors (", _commandlineoptions.numberofmirrors, ")"
		sys.exit(1)

	# RNG or parallel query without chunks activated
	if (_commandlineoptions.rng or _commandlineoptions.parallel) and not _commandlineoptions.redundancy:
		print "Chunks must be enabled and redundancy set (-r <number>) to use RNG or parallel queries!"
		sys.exit(1)

	if len(remainingargs) == 0 and _commandlineoptions.printfiles == False:
		print "Must specify at least one file to retrieve!"
		sys.exit(1)

	#filename(s)
	_commandlineoptions.filestoretrieve = remainingargs



def main():


	# If we were asked to retrieve the mainfest file, do so...
	if _commandlineoptions.retrievemanifestfrom:
		# We need to download this file...
		rawmanifestdata = raidpirlib.retrieve_rawmanifest(_commandlineoptions.retrievemanifestfrom)

		# ...make sure it is valid...
		manifestdict = raidpirlib.parse_manifest(rawmanifestdata)

		# ...and write it out if it's okay
		open(_commandlineoptions.manifestfilename, "w").write(rawmanifestdata)


	else:
		# Simply read it in from disk

		rawmanifestdata = open(_commandlineoptions.manifestfilename).read()

		manifestdict = raidpirlib.parse_manifest(rawmanifestdata)


	# we will check that the files are in the release

	# find the list of files
	manifestfilelist = raidpirlib.get_filenames_in_release(manifestdict)


	if (manifestdict['blockcount'] < _commandlineoptions.numberofmirrors * 8) and _commandlineoptions.redundancy != None:
		print "Block count too low to use chunks! Try reducing the block size or add more files to the database."
		sys.exit(1)

	if _commandlineoptions.printfiles:
		print "Manifest - Blocks:", manifestdict['blockcount'], "x", manifestdict['blocksize'], "Byte - Files:\n", manifestfilelist


	# ensure the requested files are in there...
	for filename in _commandlineoptions.filestoretrieve:

		if filename not in manifestfilelist:
			print "The file", filename, "is not listed in the manifest."
			sys.exit(2)

	# don't run PIR if we're just printing the filenames in the manifest
	if len(_commandlineoptions.filestoretrieve) > 0:
		request_files_from_mirrors(_commandlineoptions.filestoretrieve, _commandlineoptions.redundancy, _commandlineoptions.rng, _commandlineoptions.parallel, manifestdict)



if __name__ == '__main__':
	print "RAID-PIR Client", raidpirlib.pirversion

	parse_options()
	main()
