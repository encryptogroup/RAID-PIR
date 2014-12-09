""" 
<Author>
	Daniel Demmler
	(inspired from upPIR by Justin Cappos)
	(inspired from a previous version by Geremy Condra)

<Date>
	October 2014

"""


# I'll use this to XOR the result together
import simplexordatastore

# helper functions that are shared
import raidpirlib

# used for locking parallel requests
import threading

# to sleep...
import time

import sys

import socket

import session

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


def _reconstruct_block(blockinfolist):
	# private helper to reconstruct a block
		
	# xor the blocks together
	currentresult = blockinfolist[0]['xorblock']
	for xorblockdict in blockinfolist[1:]:
		currentresult = simplexordatastore.do_xor_blocks(currentresult, xorblockdict['xorblock'])

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
			if c in responses[m]['xorblockdict']:
				results[c] = simplexordatastore.do_xor_blocks(results[c], responses[m]['xorblockdict'][c]) 

	return results



class InsufficientMirrors(Exception):
	"""There are insufficient mirrors to handle your request"""

# These provide an easy way for the client XOR request behavior to be 
# modified. If you wanted to change the policy by which mirrors are selected,
# the failure behavior for offline mirrors, or the way in which blocks
# are selected.   
class RandomXORRequestor:
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




	def __init__(self, mirrorinfolist, blocklist, manifestdict, privacythreshold, pollinginterval = .1):
		"""
		<Purpose>
			Get ready to handle requests for XOR block strings, etc.

		<Arguments>
			mirrorinfolist: a list of dictionaries with information about mirrors

			blocklist: the blocks that need to be retrieved

			manifestdict: the manifest with information about the release

			privacythreshold: the number of mirrors that would need to collude to
											 break privacy

			pollinginterval: the amount of time to sleep between checking for
											 the ability to serve a mirror.   

		<Exceptions>
			TypeError may be raised if invalid parameters are given.

			InsufficientMirrors if there are not enough mirrors

		"""
		self.blocklist = blocklist
		self.manifestdict = manifestdict
		self.privacythreshold = privacythreshold
		self.pollinginterval = pollinginterval

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
			
			#open a socket once:
			thisrequestinfo['mirrorinfo']['socket'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			thisrequestinfo['mirrorinfo']['socket'].connect((mirrorinfo['ip'], mirrorinfo['port']))
			
			self.activemirrorinfolist.append(thisrequestinfo)

		for thisrequestinfo in self.activemirrorinfolist:
			#send parameters to mirrors once
			params = {}
			params['cn'] = 1 # chunk numbers, here fixed to 1
			params['k'] = privacythreshold
			params['r'] = privacythreshold
			params['cl'] = 1 # chunk length, here fixed to 1
			params['lcl'] = 1 # last chunk length, here fixed to 1
			raidpirlib.send_params(thisrequestinfo['mirrorinfo']['socket'], params)
			

		bitstringlength = raidpirlib.compute_bitstring_length(manifestdict['blockcount'])

		# let's generate the bitstrings
		for thisrequestinfo in self.activemirrorinfolist[:-1]:

			for _ in blocklist:
				thisrequestinfo['blockbitstringlist'].append(raidpirlib.randombits(manifestdict['blockcount']))

		# now, let's do the 'derived' ones...
		for blocknum in range(len(blocklist)):
			thisbitstring = '\0'*bitstringlength
			
			# xor the random strings together
			for requestinfo in self.activemirrorinfolist[:-1]:
				thisbitstring = simplexordatastore.do_xor(thisbitstring, requestinfo['blockbitstringlist'][blocknum])
	
			# ...and flip the appropriate bit for the block we want
			thisbitstring = raidpirlib.flip_bitstring_bit(thisbitstring, blocklist[blocknum])
			self.activemirrorinfolist[-1]['blockbitstringlist'].append(thisbitstring)
		
		# we're done setting up the bitstrings!


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


	def cleanup(self):
		# close sockets
		for thisrequestinfo in self.activemirrorinfolist:
			session.sendmessage(thisrequestinfo['mirrorinfo']['socket'], "Q")
			thisrequestinfo['mirrorinfo']['socket'].close()
			

	def get_next_xorrequest(self):
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

		# Three cases I need to worry about:
		#   1) nothing that still needs to be requested -> return ()
		#   2) requests remain, but all mirrors are busy -> block until ready
		#   3) there is a request ready -> return the tuple
		# 

		# I'll exit via return.   I will loop to sleep while waiting.   
		# I could use a condition variable here, but this should be fine.   There
		# should almost always be < 5 threads.   Also, why would we start more
		# threads than there are mirrors we will contact?   (As such, sleeping
		# should only happen at the very end)
		while True:
			# lock the table...
			self.tablelock.acquire()

			# but always release it
			try:
				stillserving = False
				for requestinfo in self.activemirrorinfolist:
	
					# if this mirror is serving a request, skip it...
					if requestinfo['servingrequest']:
						stillserving = True
						continue
				
					# this mirror is done...
					if len(requestinfo['blocksneeded']) == 0:
						continue
			
					# otherwise set it to be taken...
					requestinfo['servingrequest'] = True
					return (requestinfo['mirrorinfo'], requestinfo['blocksneeded'][0], requestinfo['blockbitstringlist'][0])

				if not stillserving:
					return ()

			finally:
				# I always want someone else to be able to get the lock
				self.tablelock.release()

			# otherwise, I've looked an nothing is ready...   I'll sleep and retry
			time.sleep(self.pollinginterval)
	


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
		
			# now, let's find the activemirror this corresponds ro.
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
		



	def notify_success(self, xorrequesttuple, xorblock):
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

		# acquire the lock...
		self.tablelock.acquire()
		#... but always release it
		try:
			thismirrorsinfo = xorrequesttuple[0]
		
			# now, let's find the activemirror this corresponds ro.
			for activemirrorinfo in self.activemirrorinfolist:
				if activemirrorinfo['mirrorinfo'] == thismirrorsinfo:
				
					# let's mark it as inactive and pop off the blocks, etc.
					activemirrorinfo['servingrequest'] = False
					
					# remove the block and bitstring (asserting they match what we said 
					# before)
					blocknumber = activemirrorinfo['blocksneeded'].pop(0)
					bitstring = activemirrorinfo['blockbitstringlist'].pop(0)
					assert(blocknumber == xorrequesttuple[1])
					assert(bitstring == xorrequesttuple[2])
	
					# add the xorblockinfo to the dict
					xorblockdict = {}
					xorblockdict['bitstring'] = bitstring
					xorblockdict['mirrorinfo'] = thismirrorsinfo
					xorblockdict['xorblock'] = xorblock
					self.returnedxorblocksdict[blocknumber].append(xorblockdict)

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
	
			raise Exception("InternalError: Unknown mirror in notify_failure")

		finally:
			# release the lock
			self.tablelock.release()


		

		
	def return_block(self, blocknum):
		"""
		<Purpose>
			Delivers a block.  This presumes there is sufficient cached xorblock info

		<Arguments>
			blocknum: the block number to return

		<Exceptions>
			KeyError if the block isn't known
 
		<Returns>
			The block

		"""

		return self.finishedblockdict[blocknum]
		


######################################################################



class RandomXORRequestorChunks:


	def __init__(self, mirrorinfolist, blocklist, manifestdict, privacythreshold, redundancy, rng, parallel, pollinginterval = .1):
		"""
		<Purpose>
			Get ready to handle requests for XOR block strings, etc.
			This is meant to be used for queries partitioned in chunks (parallel or SB queries with redundancy parameter)

		<Arguments>


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
			thisrequestinfo['blocksneeded'] = blocklist[:] #only for client, obviously

			if parallel:
				thisrequestinfo['parallelblocksneeded'] = []

			thisrequestinfo['blockchunklist'] = []
			
			
			if rng:
				thisrequestinfo['seedlist'] = []

			# chunk numbers [0, ..., r-1]
			thisrequestinfo['chunknumbers'] = [i]
			for j in xrange(1, redundancy):
				thisrequestinfo['chunknumbers'].append((i+j) % privacythreshold)
			i = i + 1
			
			#open a socket once:
			thisrequestinfo['mirrorinfo']['socket'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			thisrequestinfo['mirrorinfo']['socket'].connect((mirrorinfo['ip'], mirrorinfo['port']))
			
			self.activemirrorinfolist.append(thisrequestinfo)

		for thisrequestinfo in self.activemirrorinfolist:
			#send parameters to mirrors once
			params = {}
			params['cn'] = thisrequestinfo['chunknumbers']
			params['k'] = privacythreshold
			params['r'] = redundancy
			params['cl'] = self.chunklen
			params['lcl'] = self.lastchunklen
			raidpirlib.send_params(thisrequestinfo['mirrorinfo']['socket'], params)

		
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

					if rng:
						#pick a random seed (key) and initialize AES
						seed = _randomnumberfunction(16)
						thisrequestinfo['seedlist'].append(seed)
						raidpirlib.initAES(seed)

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
							chunks[c] = raidpirlib.nextrandombitsAES(length)

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
					thisbitstring = raidpirlib.compute_bitstring_length(length)*'\0'

					#xor all other rnd chunks onto it
					for rqi in self.activemirrorinfolist:
						if c in rqi['blockchunklist'][-1]:
							thisbitstring = simplexordatastore.do_xor(thisbitstring, rqi['blockchunklist'][-1][c])
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

					if rng:
						seed = _randomnumberfunction(16)
						thisrequestinfo['seedlist'].append(seed)
						raidpirlib.initAES(seed)
										
					chunks = {}

					#iterate through r-1 random chunks
					for c in thisrequestinfo['chunknumbers'][1:]:
						
						#pick correct length in bits
						if c == self.privacythreshold - 1:
							length = self.lastchunklen
						else:
							length = self.chunklen

						if rng:
							chunks[c] = raidpirlib.nextrandombitsAES(length)

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
					thisbitstring = raidpirlib.compute_bitstring_length(length)*'\0'

					#xor all other rnd chunks onto it
					for rqi in self.activemirrorinfolist:
						if c in rqi['blockchunklist'][-1]:
							thisbitstring = simplexordatastore.do_xor(thisbitstring, rqi['blockchunklist'][-1][c])
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


	def cleanup(self):
		# close sockets
		for thisrequestinfo in self.activemirrorinfolist:
			session.sendmessage(thisrequestinfo['mirrorinfo']['socket'], "Q")
			thisrequestinfo['mirrorinfo']['socket'].close()

	# chunked version:
	def get_next_xorrequest(self):
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

		# Three cases I need to worry about:
		#   1) nothing that still needs to be requested -> return ()
		#   2) requests remain, but all mirrors are busy -> block until ready
		#   3) there is a request ready -> return the tuple
		# 

		# I'll exit via return.   I will loop to sleep while waiting.   
		# I could use a condition variable here, but this should be fine.   There
		# should almost always be < 5 threads.   Also, why would we start more
		# threads than there are mirrors we will contact?   (As such, sleeping
		# should only happen at the very end)
		while True:
			# lock the table...
			self.tablelock.acquire()

			# but always release it
			try:
				stillserving = False
				for requestinfo in self.activemirrorinfolist:
	
				
					# if this mirror is serving a request, skip it...
					if requestinfo['servingrequest']:
						stillserving = True
						continue
					

					if self.parallel:
						if len(requestinfo['parallelblocksneeded']) == 0:
							continue
						# otherwise set it to be taken...
						requestinfo['servingrequest'] = True

						if self.rng:
							return (requestinfo['mirrorinfo'], requestinfo['parallelblocksneeded'][0], requestinfo['blockchunklist'][0], 2, requestinfo['seedlist'][0])
						else:
							raise Exception("Parallel Query without RNG not yet implemented!") #TODO

					#single block
					else: 
						# this mirror is done...
						if len(requestinfo['blocksneeded']) == 0:
							continue
				
						# otherwise set it to be taken...
						requestinfo['servingrequest'] = True

						if self.rng:
							return (requestinfo['mirrorinfo'], requestinfo['blocksneeded'][0], requestinfo['blockchunklist'][0], 1, requestinfo['seedlist'][0])
						else:
							return (requestinfo['mirrorinfo'], requestinfo['blocksneeded'][0], requestinfo['blockchunklist'][0], 0)

				if not stillserving:
					return ()

			finally:
				# I always want someone else to be able to get the lock
				self.tablelock.release()

			# otherwise, I've looked an nothing is ready...   I'll sleep and retry
			time.sleep(self.pollinginterval)
	




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
		
			# now, let's find the activemirror this corresponds ro.
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
		



	def notify_success(self, xorrequesttuple, xorblock):
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

		# acquire the lock...
		self.tablelock.acquire()

		try:
			thismirrorsinfo = xorrequesttuple[0]
		
			# now, let's find the activemirror this corresponds ro.
			for activemirrorinfo in self.activemirrorinfolist:
				if activemirrorinfo['mirrorinfo'] == thismirrorsinfo:
				
					# let's mark it as inactive and pop off the blocks, etc.
					activemirrorinfo['servingrequest'] = False
					

					if self.parallel:
						#use blocknumbers[0] as index from now on
						blocknumbers = activemirrorinfo['parallelblocksneeded'].pop(0)

						activemirrorinfo['blockchunklist'].pop(0) 

						if self.rng:
							seed = activemirrorinfo['seedlist'].pop(0) 
						#assert(blocknumber == xorrequesttuple[1]) #TODO modify this checks for parallel query [this one contains the blocknumbers]
						#assert(bitstring == xorrequesttuple[2])
		
						# add the xorblockinfo to the dict
						xorblockdict = {}
						#xorblockdict['bitstring'] = xorrequesttuple[2]
						xorblockdict['mirrorinfo'] = thismirrorsinfo
						xorblockdict['xorblockdict'] = msgpack.unpackb(xorblock) #the mirror response: dict r blocks, index = chunk number
						self.returnedxorblocksdict[blocknumbers[0]].append(xorblockdict)

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
						blocknumber = activemirrorinfo['blocksneeded'].pop(0)

						bitstring = activemirrorinfo['blockchunklist'].pop(0) 
						if self.rng:
							seed = activemirrorinfo['seedlist'].pop(0) 
						assert(blocknumber == xorrequesttuple[1])
						#assert(bitstring == xorrequesttuple[2])
		
						# add the xorblockinfo to the dict
						xorblockdict = {}
						#xorblockdict['bitstring'] = xorrequesttuple[2]
						xorblockdict['mirrorinfo'] = thismirrorsinfo
						xorblockdict['xorblock'] = xorblock
						self.returnedxorblocksdict[blocknumber].append(xorblockdict)

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
	
			raise Exception("InternalError: Unknown mirror in notify_failure")

		finally:
			# release the lock
			self.tablelock.release()

	
	def return_block(self, blocknum):
		"""
		<Purpose>
			Delivers a block.  This presumes there is sufficient cached xorblock info

		<Arguments>
			blocknum: the block number to return

		<Exceptions>
			KeyError if the block isn't known
 
		<Returns>
			The block

		"""
		return self.finishedblockdict[blocknum]
		