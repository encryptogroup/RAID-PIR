"""
<Author>
	Daniel Demmler
	(inspired from upPIR by Justin Cappos)
	(inspired from a previous version by Geremy Condra)

<Date>
	December 2014

"""

# I'll use this to XOR the result together
import fastsimplexordatastore as xordatastore

# helper functions that are shared
import raidpirlib

# used for locking parallel requests
import threading

import sys

import socket

import session

# to sleep...
_timer = raidpirlib._timer

try:
	#for packing more complicated messages
	import msgpack
except ImportError:
	print "Requires MsgPack module (http://msgpack.org/)"
	sys.exit(1)

import os
_randomnumberfunction = os.urandom

# used for mirror selection...
import random


########################### XORRequestGenerator ###############################

# receive thread
def rcvlet(requestinfo, rxgobj):
	sock = requestinfo['mirrorinfo']['socket']

	# first, check if params were received correctly
	if session.recvmessage(sock) != 'PARAMS OK':
		raise Exception("Params were not delivered correctly or wrong format.")

	data = "0"
	first = True
	while data != '' and len(requestinfo['blocksrequested']) > 0 or first:
		first = False
		data = session.recvmessage(sock)
		rxgobj.notify_success(requestinfo['mirrorinfo'], data)


def _reconstruct_block(blockinfolist):
	# private helper to reconstruct a block

	# xor the blocks together
	currentresult = blockinfolist[0]
	for xorblock in blockinfolist[1:]:
		currentresult = xordatastore.do_xor(currentresult, xorblock)

	# and return the answer
	return currentresult


def _reconstruct_block_parallel(responses, chunklen, k, blocklen, blocknumbers):
	#reconstruct block(s) from parallel query answer

	results = {}
	for blocknum in blocknumbers:
		#map blocknum to chunk
		index = min(blocknum/chunklen, k-1)

		if index not in results:
			results[index] = blocklen*"\0"

	for m in range(k):
		for c in results:
			if c in responses[m]:
				results[c] = xordatastore.do_xor(results[c], responses[m][c])

	return results


class InsufficientMirrors(Exception):
	"""There are insufficient mirrors to handle your request"""


# Super class of requestors that offers identical functions
class Requestor(object):


	def cleanup(self):
		"""cleanup. here: maybe request debug timing info and always close sockets"""
		for thisrequestinfo in self.activemirrorinfolist:

			if self.timing:
				# request total computation time and measure delay
				ping_start = _timer()
				session.sendmessage(thisrequestinfo['mirrorinfo']['socket'], "T")
				thisrequestinfo['mirrorinfo']['comptime'] = float(session.recvmessage(thisrequestinfo['mirrorinfo']['socket'])[1:])
				thisrequestinfo['mirrorinfo']['ping'] = _timer() - ping_start

			session.sendmessage(thisrequestinfo['mirrorinfo']['socket'], "Q")
			thisrequestinfo['mirrorinfo']['socket'].close()


	def return_timings(self):
		comptimes = []
		pings = []
		for rqi in self.activemirrorinfolist:
			comptimes.append(rqi['mirrorinfo']['comptime'])
			pings.append(rqi['mirrorinfo']['ping'])

		return self.recons_time, comptimes, pings


	def return_block(self, blocknum):
		return self.finishedblockdict[blocknum]



# These provide an easy way for the client XOR request behavior to be
# modified. If you wanted to change the policy by which mirrors are selected,
# the failure behavior for offline mirrors, or the way in which blocks
# are selected.
class RandomXORRequestor(Requestor):
	"""
	<Purpose>
		Basic XORRequestGenerator that just picks some number of random mirrors
		and then retrieves all blocks from them. If any mirror fails or is
		offline, the operation fails.

		The strategy this uses is very, very simple. First we randomly choose
		$k$ mirrors we want to retrieve blocks from. If at any point, we have
		a failure when retrieving a block, we replace that mirror with a
		mirror we haven't chosen yet.

	<Side Effects>
		None.

	<Example Use>
		>>> rxgobj = RandomXORRequestor(['mirror1','mirror2','mirror3'],
						 [23, 45], { ...# manifest dict omitted # }, 2)

		>>> print rxgobj.get_next_xorrequest()
		('mirror3',23, '...')   # bitstring omitted
		>>> print rxgobj.get_next_xorrequest()
		('mirror1',23, '...')   # bitstring omitted
		>>> print rxgobj.get_next_xorrequest()
		# this will block because we didn't say either of the others
		# completed and there are no other mirrors waiting

		>>> rxgobj.notify_success(('mirror1',23,'...'), '...')
		# the bit string and result were omitted from the previous statement
		>>> print rxgobj.get_next_xorrequest()
		('mirror1',45, '...')   # bitstring omitted
		>>> rxgobj.notify_success(('mirror3',23, '...'), '...')
		>>> print rxgobj.get_next_xorrequest()
		('mirror1',45, '...')   # bitstring omitted
		>>> rxgobj.notify_failure(('mirror1',45, '...'))
		>>> print rxgobj.get_next_xorrequest()
		('mirror2',45, '...')
		>>> rxgobj.notify_success(('mirror2',45, '...'), '...')
		>>> print rxgobj.get_next_xorrequest()
		()

	"""


	def __init__(self, mirrorinfolist, blocklist, manifestdict, privacythreshold, batch, timing, pollinginterval=.1):
		"""
		<Purpose>
			Get ready to handle requests for XOR block strings, etc.

		<Arguments>
			mirrorinfolist: a list of dictionaries with information about mirrors

			blocklist: the blocks that need to be retrieved

			manifestdict: the manifest with information about the release

			privacythreshold: the number of mirrors that would need to collude to break privacy

			timing: collect timing info

			pollinginterval: the amount of time to sleep between checking for the ability to serve a mirror.

		<Exceptions>
			TypeError may be raised if invalid parameters are given.

			InsufficientMirrors if there are not enough mirrors

		"""
		self.blocklist = blocklist
		self.manifestdict = manifestdict
		self.privacythreshold = privacythreshold
		self.pollinginterval = pollinginterval
		self.timing = timing
		if timing:
			self.recons_time = 0

		if len(mirrorinfolist) < self.privacythreshold:
			raise InsufficientMirrors("Requested the use of "+str(self.privacythreshold)+" mirrors, but only "+str(len(mirrorinfolist))+" were available.")

		# now we do the 'random' part.   I copy the mirrorinfolist to avoid changing the list in place.
		self.fullmirrorinfolist = mirrorinfolist[:]
		random.shuffle(self.fullmirrorinfolist)

		# let's make a list of mirror information (what has been retrieved, etc.)
		self.activemirrorinfolist = []
		for mirrorinfo in self.fullmirrorinfolist[:self.privacythreshold]:
			thisrequestinfo = {}
			thisrequestinfo['mirrorinfo'] = mirrorinfo
			thisrequestinfo['servingrequest'] = False
			thisrequestinfo['blocksneeded'] = blocklist[:]
			thisrequestinfo['blockbitstringlist'] = []
			thisrequestinfo['blocksrequested'] = []

			# open a socket once:
			thisrequestinfo['mirrorinfo']['socket'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			thisrequestinfo['mirrorinfo']['socket'].setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) #TODO check this in the cloud
			thisrequestinfo['mirrorinfo']['socket'].connect((mirrorinfo['ip'], mirrorinfo['port']))

			self.activemirrorinfolist.append(thisrequestinfo)

		for thisrequestinfo in self.activemirrorinfolist:
			#send parameters to mirrors once
			params = {}
			params['cn'] = 1 # chunk numbers, here fixed to 1
			params['k'] = privacythreshold
			params['r'] = privacythreshold # r is irrelevant here, thus fixed to k
			params['cl'] = 1 # chunk length, here fixed to 1
			params['lcl'] = 1 # last chunk length, here fixed to 1
			params['b'] = batch
			params['p'] = False

			#send the params, rcvlet will check response
			session.sendmessage(thisrequestinfo['mirrorinfo']['socket'], "P" + msgpack.packb(params))

			# start separate receiving thread for this socket
			t = threading.Thread(target=rcvlet, args=[thisrequestinfo, self], name=("rcv_thread_" + str((thisrequestinfo['mirrorinfo']['ip'], thisrequestinfo['mirrorinfo']['port']))))
			thisrequestinfo['rt'] = t
			t.start()

		bitstringlength = raidpirlib.bits_to_bytes(manifestdict['blockcount'])

		# let's generate the random bitstrings for k-1 mirrors
		for thisrequestinfo in self.activemirrorinfolist[:-1]:

			for _ in blocklist:
				thisrequestinfo['blockbitstringlist'].append(raidpirlib.randombits(manifestdict['blockcount']))

		# now, let's do the 'derived' ones...
		for blocknum in xrange(len(blocklist)):
			thisbitstring = '\0'*bitstringlength

			# xor the random strings together
			for requestinfo in self.activemirrorinfolist[:-1]:
				thisbitstring = xordatastore.do_xor(thisbitstring, requestinfo['blockbitstringlist'][blocknum])

			# flip the appropriate bit for the block we want
			thisbitstring = raidpirlib.flip_bitstring_bit(thisbitstring, blocklist[blocknum])

			# store the result for the last mirror
			self.activemirrorinfolist[-1]['blockbitstringlist'].append(thisbitstring)

		# want to have a structure for locking
		self.tablelock = threading.Lock()

		# and we'll keep track of the ones that are waiting in the wings...
		self.backupmirrorinfolist = self.fullmirrorinfolist[self.privacythreshold:]

		# the returned blocks are put here...
		self.returnedxorblocksdict = {}
		for blocknum in blocklist:
			# make these all empty lists to start with
			self.returnedxorblocksdict[blocknum] = []

		# and here is where they are put when reconstructed
		self.finishedblockdict = {}

		# and we're ready!


	def get_next_xorrequest(self, tid):
		"""
		<Purpose>
			Gets the next requesttuple that should be returned

		<Arguments>
			None

		<Exceptions>
			InsufficientMirrors if there are not enough mirrors

		<Returns>
			Either a requesttuple (mirrorinfo, blocknumber, bitstring) or ()
			when all strings have been retrieved...

		"""

		# Two cases I need to worry about:
		#   1) nothing that still needs to be requested -> return ()
		#   2) there is a request ready -> return the request

		requestinfo = self.activemirrorinfolist[tid]

		# this mirror is done...
		if len(requestinfo['blocksneeded']) == 0:
			return ()

		# otherwise set it to be taken...
		requestinfo['servingrequest'] = True
		blocknum = requestinfo['blocksneeded'].pop()
		requestinfo['blocksrequested'].append(blocknum)

		return (requestinfo['mirrorinfo'], blocknum, requestinfo['blockbitstringlist'].pop())


	def notify_failure(self, xorrequesttuple):
		"""
		<Purpose>
			Handles that a mirror has failed

		<Arguments>
			The XORrequesttuple that was returned by get_next_xorrequest

		<Exceptions>
			InsufficientMirrors if there are not enough mirrors

			An internal error is raised if the XORrequesttuple is bogus

		<Returns>
			None

		"""
		# I should lock the table...
		self.tablelock.acquire()

		# but *always* release it
		try:
			# if we're out of replacements, quit
			if len(self.backupmirrorinfolist) == 0:
				raise InsufficientMirrors("There are no replacement mirrors")

			nextmirrorinfo = self.backupmirrorinfolist.pop(0)

			failedmirrorsinfo = xorrequesttuple[0]

			# now, let's find the activemirror this corresponds to.
			for activemirrorinfo in self.activemirrorinfolist:
				if activemirrorinfo['mirrorinfo'] == failedmirrorsinfo:

					# let's mark it as inactive and set up a different mirror
					activemirrorinfo['mirrorinfo'] = nextmirrorinfo
					activemirrorinfo['servingrequest'] = False
					return

			raise Exception("InternalError: Unknown mirror in notify_failure")

		finally:
			# release the lock
			self.tablelock.release()


	def notify_success(self, thismirrorsinfo, xorblock):
		"""
		<Purpose>
			Handles the receipt of an xorblock

		<Arguments>
			xorrequesttuple: The tuple that was returned by get_next_xorrequest

			xorblock: the data returned by the mirror

		<Exceptions>
			Assertions / IndexError / TypeError / InternalError if the
			XORrequesttuple is bogus

		<Returns>
			None

		"""

		if self.timing:
			stime = _timer()

		# acquire the lock...
		self.tablelock.acquire()
		#... but always release it
		try:

			# now, let's find the activemirror this corresponds to.
			for activemirrorinfo in self.activemirrorinfolist:
				if activemirrorinfo['mirrorinfo'] == thismirrorsinfo:

					# remove the block and bitstring (asserting they match what we said before)
					blocknumber = activemirrorinfo['blocksrequested'].pop(0)

					# add the xorblockinfo to the dict
					self.returnedxorblocksdict[blocknumber].append(xorblock)

					# if we don't have all of the pieces, continue
					if len(self.returnedxorblocksdict[blocknumber]) != self.privacythreshold:
						return

					# if we have all of the pieces, reconstruct it
					resultingblock = _reconstruct_block(self.returnedxorblocksdict[blocknumber])

					# let's check the hash...
					resultingblockhash = raidpirlib.find_hash(resultingblock, self.manifestdict['hashalgorithm'])
					if resultingblockhash != self.manifestdict['blockhashlist'][blocknumber]:
						# TODO: We should notify the vendor!
						raise Exception('Should notify vendor that one of the mirrors or manifest is corrupt')

					# otherwise, let's put this in the finishedblockdict
					self.finishedblockdict[blocknumber] = resultingblock

					# it should be safe to delete this
					del self.returnedxorblocksdict[blocknumber]
					return

			raise Exception("InternalError: Unknown mirror in notify_success")

		finally:
			# release the lock
			self.tablelock.release()
			if self.timing:
				self.recons_time = self.recons_time + _timer() - stime


######################################################################


class RandomXORRequestorChunks(Requestor):

	def __init__(self, mirrorinfolist, blocklist, manifestdict, privacythreshold, redundancy, rng, parallel, batch, timing, pollinginterval=.1):
		"""
		<Purpose>
			Get ready to handle requests for XOR block strings, etc.
			This is meant to be used for queries partitioned in chunks (parallel or SB queries with redundancy parameter)

		<Exceptions>
			TypeError may be raised if invalid parameters are given.

			InsufficientMirrors if there are not enough mirrors

		"""

		self.blocklist = blocklist
		self.manifestdict = manifestdict
		self.privacythreshold = privacythreshold # aka k, the number of mirrors to use
		self.pollinginterval = pollinginterval
		self.redundancy = redundancy #aka r
		self.rng = rng
		self.parallel = parallel
		self.blockcount = manifestdict['blockcount']
		self.timing = timing
		if timing:
			self.recons_time = 0

		#length of one chunk in BITS (1 bit per block)
		#chunk length of the first chunks must be a multiple of 8, last chunk can be longer than first chunks
		self.chunklen = (self.blockcount/8/privacythreshold) * 8
		self.lastchunklen = self.blockcount - (privacythreshold-1)*self.chunklen

		if len(mirrorinfolist) < self.privacythreshold:
			raise InsufficientMirrors("Requested the use of "+str(self.privacythreshold)+" mirrors, but only "+str(len(mirrorinfolist))+" were available.")

		# now we do the 'random' part. I copy the mirrorinfolist to avoid changing the list in place.
		self.fullmirrorinfolist = mirrorinfolist[:]
		random.shuffle(self.fullmirrorinfolist)


		# let's make a list of mirror information (what has been retrieved, etc.)
		self.activemirrorinfolist = []

		#initialize queries for mirrors
		i = 0
		for mirrorinfo in self.fullmirrorinfolist[:self.privacythreshold]:
			thisrequestinfo = {}
			thisrequestinfo['mirrorinfo'] = mirrorinfo
			thisrequestinfo['servingrequest'] = False
			thisrequestinfo['blocksneeded'] = blocklist[:] # only for the client, obviously
			thisrequestinfo['blocksrequested'] = []

			if parallel:
				thisrequestinfo['parallelblocksneeded'] = []

			thisrequestinfo['blockchunklist'] = []


			# chunk numbers [0, ..., r-1]
			thisrequestinfo['chunknumbers'] = [i]
			for j in xrange(1, redundancy):
				thisrequestinfo['chunknumbers'].append((i+j) % privacythreshold)
			i = i + 1

			#open a socket once:
			thisrequestinfo['mirrorinfo']['socket'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			thisrequestinfo['mirrorinfo']['socket'].connect((mirrorinfo['ip'], mirrorinfo['port']))

			if rng:
				#pick a random seed (key) and initialize AES
				seed = _randomnumberfunction(16) # random 128 bit key
				thisrequestinfo['seed'] = seed
				thisrequestinfo['cipher'] = raidpirlib.initAES(seed)

			self.activemirrorinfolist.append(thisrequestinfo)

		for thisrequestinfo in self.activemirrorinfolist:
			#send parameters to mirrors once
			params = {}
			params['cn'] = thisrequestinfo['chunknumbers']
			params['k'] = privacythreshold
			params['r'] = redundancy
			params['cl'] = self.chunklen
			params['lcl'] = self.lastchunklen
			params['b'] = batch
			params['p'] = parallel

			if rng:
				params['s'] = thisrequestinfo['seed']

			#send the params, rcvlet will check response
			session.sendmessage(thisrequestinfo['mirrorinfo']['socket'], "P" + msgpack.packb(params))

			# start separate receiving thread for this socket
			t = threading.Thread(target=rcvlet, args=[thisrequestinfo, self], name=("rcv_thread_" + str((thisrequestinfo['mirrorinfo']['ip'], thisrequestinfo['mirrorinfo']['port']))))
			thisrequestinfo['rt'] = t
			t.start()


		#multi block query. map the blocks to the minimum amount of queries
		if parallel:

			#create dictionary for each chunk, will hold block indices per chunk
			blockchunks = {}
			for i in range(0, privacythreshold):
				blockchunks[i] = []

			#map block numbers to chunks
			for blocknum in blocklist:
				index = min(blocknum/self.chunklen, privacythreshold-1)
				blockchunks[index].append(blocknum)

			#remove chunks that are still empty
			for i in range(0, privacythreshold):
				if len(blockchunks[i]) == 0:
					del blockchunks[i]

			#do until all blocks are in queries
			while len(blockchunks)>0:

				#iterate through mirrors
				for thisrequestinfo in self.activemirrorinfolist:

					#dicitonary of chunk requests
					chunks = {}

					#iterate through r-1 random chunks, skipping the head (flip) chunk
					for c in thisrequestinfo['chunknumbers'][1:]:

						#pick correct length in bits
						if c == self.privacythreshold - 1:
							length = self.lastchunklen
						else:
							length = self.chunklen

						if rng:
							#set random bytes for the latter chunk(s) from AES (will be deleted later)
							chunks[c] = raidpirlib.nextrandombitsAES(thisrequestinfo['cipher'], length)

						else:
							#set random bytes for the latter chunk(s) randomly
							chunks[c] = raidpirlib.randombits(length)

					thisrequestinfo['blockchunklist'].append(chunks)

				#list of blocknumbers
				blocks = []

				# now derive the first chunks
				for thisrequestinfo in self.activemirrorinfolist:

					#number of the first chunk
					c = thisrequestinfo['chunknumbers'][0]

					#pick correct length for the chunk
					if c == self.privacythreshold - 1:
						length = self.lastchunklen
					else:
						length = self.chunklen

					#fill it with zero
					thisbitstring = raidpirlib.bits_to_bytes(length)*'\0'

					#xor all other rnd chunks onto it
					for rqi in self.activemirrorinfolist:
						if c in rqi['blockchunklist'][-1]:
							thisbitstring = xordatastore.do_xor(thisbitstring, rqi['blockchunklist'][-1][c])
							if rng:
								del rqi['blockchunklist'][-1][c] #remove the pre-computed random chunk from the packet to send

					#if there is a block within this chunk, then add it to the bitstring by flipping the bit
					if c in blockchunks:
						blocknum = blockchunks[c].pop(0)
						thisbitstring = raidpirlib.flip_bitstring_bit(thisbitstring, blocknum - c*self.chunklen)
						blocks.append(blocknum)
						if len(blockchunks[c]) == 0:
							del blockchunks[c]

					thisrequestinfo['parallelblocksneeded'].append(blocks)
					thisrequestinfo['blockchunklist'][-1][c] = thisbitstring


		#single block query:
		else:
			#iterate through all blocks
			for blocknum in blocklist:

				#iterate through mirrors
				for thisrequestinfo in self.activemirrorinfolist:

					chunks = {}

					#iterate through r-1 random chunks
					for c in thisrequestinfo['chunknumbers'][1:]:

						#pick correct length in bits
						if c == self.privacythreshold - 1:
							length = self.lastchunklen
						else:
							length = self.chunklen

						if rng:
							chunks[c] = raidpirlib.nextrandombitsAES(thisrequestinfo['cipher'], length)

						else:
							#set random bytes for the latter chunk(s)
							chunks[c] = raidpirlib.randombits(length)

					thisrequestinfo['blockchunklist'].append(chunks)

				# now derive the first chunks
				for thisrequestinfo in self.activemirrorinfolist:

					#number of the first chunk
					c = thisrequestinfo['chunknumbers'][0]

					#pick correct length for the chunk
					if c == self.privacythreshold - 1:
						length = self.lastchunklen
					else:
						length = self.chunklen

					#fill it with zero
					thisbitstring = raidpirlib.bits_to_bytes(length)*'\0'

					#xor all other rnd chunks onto it
					for rqi in self.activemirrorinfolist:
						if c in rqi['blockchunklist'][-1]:
							thisbitstring = xordatastore.do_xor(thisbitstring, rqi['blockchunklist'][-1][c])
							if rng:
								del rqi['blockchunklist'][-1][c] #remove the pre-computed random chunk from the packet to send

					#if the desired block is within this chunk, flip the bit
					if c*self.chunklen <= blocknum and blocknum < c*self.chunklen + length:
						thisbitstring = raidpirlib.flip_bitstring_bit(thisbitstring, blocknum - c*self.chunklen)

					thisrequestinfo['blockchunklist'][-1][c] = thisbitstring


		########################################

		# want to have a structure for locking
		self.tablelock = threading.Lock()

		# and we'll keep track of the ones that are waiting in the wings...
		self.backupmirrorinfolist = self.fullmirrorinfolist[self.privacythreshold:]

		# the returned blocks are put here...
		self.returnedxorblocksdict = {}
		for blocknum in blocklist:
			# make these all empty lists to start with
			self.returnedxorblocksdict[blocknum] = []

		# and here is where they are put when reconstructed
		self.finishedblockdict = {}

		# preparation done. queries are ready to be sent.


	# chunked version:
	def get_next_xorrequest(self, tid):
		"""
		<Purpose>
			Gets the next request tuple that should be returned

		<Arguments>
			None

		<Exceptions>
			InsufficientMirrors if there are not enough mirrors

		<Returns>
			Either a requesttuple (mirrorinfo, blocknumber, bitstring) or ()
			when all strings have been retrieved...

		"""

		requestinfo = self.activemirrorinfolist[tid]


		if self.parallel:
			if len(requestinfo['parallelblocksneeded']) == 0:
				return ()
			# otherwise set it to be taken...
			requestinfo['servingrequest'] = True

			blocknums = requestinfo['parallelblocksneeded'].pop(0)
			requestinfo['blocksrequested'].append(blocknums)

			if self.rng:
				return (requestinfo['mirrorinfo'], blocknums, requestinfo['blockchunklist'].pop(0), 2)
			else:
				raise Exception("Parallel Query without RNG not yet implemented!")

		#single block
		else:
			# this mirror is done...
			if len(requestinfo['blocksneeded']) == 0:
				return ()

			# otherwise set it to be taken...
			requestinfo['servingrequest'] = True

			blocknum = requestinfo['blocksneeded'].pop(0)
			requestinfo['blocksrequested'].append(blocknum)

			if self.rng:
				return (requestinfo['mirrorinfo'], blocknum, requestinfo['blockchunklist'].pop(0), 1)
			else:
				return (requestinfo['mirrorinfo'], blocknum, requestinfo['blockchunklist'].pop(0), 0)


	def notify_failure(self, xorrequesttuple):
		"""
		<Purpose>
			Handles that a mirror has failed

		<Arguments>
			The XORrequesttuple that was returned by get_next_xorrequest

		<Exceptions>
			InsufficientMirrors if there are not enough mirrors

			An internal error is raised if the XORrequesttuple is bogus

		<Returns>
			None

		"""
		# I should lock the table...
		self.tablelock.acquire()

		# but *always* release it
		try:
			# if we're out of replacements, quit
			if len(self.backupmirrorinfolist) == 0:
				raise InsufficientMirrors("There are no replacement mirrors")

			nextmirrorinfo = self.backupmirrorinfolist.pop(0)

			failedmirrorsinfo = xorrequesttuple[0]

			# now, let's find the activemirror this corresponds to.
			for activemirrorinfo in self.activemirrorinfolist:
				if activemirrorinfo['mirrorinfo'] == failedmirrorsinfo:

					# let's mark it as inactive and set up a different mirror
					activemirrorinfo['mirrorinfo'] = nextmirrorinfo
					activemirrorinfo['servingrequest'] = False
					return

			raise Exception("InternalError: Unknown mirror in notify_failure")

		finally:
			# release the lock
			self.tablelock.release()


	def notify_success(self, thismirrorsinfo, xorblock):
		"""
		<Purpose>
			Handles the receipt of an xorblock

		<Arguments>
			xorrequesttuple: The tuple that was returned by get_next_xorrequest

			xorblock: the data returned by the mirror

		<Exceptions>
			Assertions / IndexError / TypeError / InternalError if the
			XORrequesttuple is bogus

		<Returns>
			None

		"""

		if self.timing:
			stime = _timer()

		# acquire the lock...
		self.tablelock.acquire()

		try:

			# now, let's find the activemirror this corresponds to.
			for activemirrorinfo in self.activemirrorinfolist:
				if activemirrorinfo['mirrorinfo'] == thismirrorsinfo:

					if self.parallel:
						#use blocknumbers[0] as index from now on
						blocknumbers = activemirrorinfo['blocksrequested'].pop(0)

						# add the xorblocks to the dict
						self.returnedxorblocksdict[blocknumbers[0]].append(msgpack.unpackb(xorblock))

						#print "Appended blocknumber", blocknumbers[0], "from", thismirrorsinfo['port']

						# if we don't have all of the pieces, continue
						if len(self.returnedxorblocksdict[blocknumbers[0]]) != self.privacythreshold:
							return

						# if we have all of the pieces, reconstruct it
						resultingblockdict = _reconstruct_block_parallel(self.returnedxorblocksdict[blocknumbers[0]], self.chunklen, self.privacythreshold, self.manifestdict['blocksize'], blocknumbers)

						#parse resultingblocks into single blocks


						for blocknumber in blocknumbers:

							index = min(blocknumber/self.chunklen, self.privacythreshold-1)

							# let's check the hash...
							resultingblockhash = raidpirlib.find_hash(resultingblockdict[index], self.manifestdict['hashalgorithm'])
							if resultingblockhash != self.manifestdict['blockhashlist'][blocknumber]:
								print activemirrorinfo
								# TODO: We should notify the vendor!
								raise Exception('Should notify vendor that one of the mirrors or manifest is corrupt, for blocknumber ' + str(blocknumber))

							# otherwise, let's put this in the finishedblockdict
							self.finishedblockdict[blocknumber] = resultingblockdict[index]


						# it should be safe to delete this
						del self.returnedxorblocksdict[blocknumbers[0]]

						return


					#single block query:
					else:
					# remove the block and bitstring (asserting they match what we said before)
						blocknumber = activemirrorinfo['blocksrequested'].pop(0)

						# add the xorblock to the dict
						self.returnedxorblocksdict[blocknumber].append(xorblock)

						# if we don't have all of the pieces, continue
						if len(self.returnedxorblocksdict[blocknumber]) != self.privacythreshold:
							return

						# if we have all of the pieces, reconstruct it
						resultingblock = _reconstruct_block(self.returnedxorblocksdict[blocknumber])

						# let's check the hash...
						resultingblockhash = raidpirlib.find_hash(resultingblock, self.manifestdict['hashalgorithm'])
						if resultingblockhash != self.manifestdict['blockhashlist'][blocknumber]:
							print activemirrorinfo
							# TODO: We should notify the vendor!
							raise Exception('Should notify vendor that one of the mirrors or manifest is corrupt')

						# otherwise, let's put this in the finishedblockdict
						self.finishedblockdict[blocknumber] = resultingblock

						# it should be safe to delete this
						del self.returnedxorblocksdict[blocknumber]
						return

			raise Exception("InternalError: Unknown mirror in notify_success")

		finally:
			# release the lock
			self.tablelock.release()
			if self.timing:
				self.recons_time = self.recons_time + _timer() - stime
