#!/usr/bin/env python2
"""
<Author>
	Daniel Demmler
	(inspired from upPIR by Justin Cappos)
	(inspired from a previous version by Geremy Condra)

<Date>
	December 2014

<Description>
	Mirror code that serves RAID-PIR files.   A client obtains a list of live
	mirrors from a vendor.   The client then sends bit strings to the mirror and
	the mirror returns XORed blocks of data.   The mirror will periodically
	announce its liveness to the vendor it is serving content for.

	The files specified in the manifest must already exist on the local machine.

	For more technical explanation, please see the paper.

<Options>
	See below...
"""


# This file is laid out in four main parts.   First, there are some helper
# functions to advertise the mirror with the vendor.   The second section
# includes the functionality to serve content via RAID-PIR.   The third section
# serves data via HTTP.   The final part contains the option
# parsing and main.   To get an overall feel for the code, it is recommended
# to follow the execution from main on.
#
# EXTENSION POINTS:
#
# One can define new xordatastore types (although I need a more explicit plugin
# module).   These could include memoization and other optimizations to
# further improve the speed of XOR processing.


import sys

import traceback

import optparse

# Holds the mirror data and produces the XORed blocks
import fastsimplexordatastore

# if you cannot build the C datastore uncomment the following line
#import simplexordatastore as fastsimplexordatastore

# helper functions that are shared
import raidpirlib as lib

# This is used to communicate with clients with a message like abstraction
import session

# used to start the RAID-PIR servers in parallel
import threading

# to automatically find the public ip
import getmyip

# to handle protocol requests
import SocketServer

# to run in the background...
import daemon

# for logging purposes...
import time

# for unpacking messages
try:
	import msgpack
except ImportError:
	print "Requires MsgPack module (http://msgpack.org/)"
	sys.exit(1)

_logfo = None
_timer = time.time

def _log(stringtolog):
	"""helper function to log data"""
	_logfo.write(str(time.time()) +" "+stringtolog+"\n")
	_logfo.flush()

_global_myxordatastore = None
_global_manifestdict = None
_request_restart = False


#################### Advertising ourself with the vendor ######################
def _send_mirrorinfo():
	"""private function that sends our mirrorinfo to the vendor"""


	# adding more information here  is a natural way to extend the mirror /
	# client.   The vendor should not need to be changed
	# at a minimum, the 'ip' and 'port' are required to provide the client with
	# information about how to contact the mirror.
	mymirrorinfo = {'ip':_commandlineoptions.ip, 'port':_commandlineoptions.port}

	if _commandlineoptions.vendorip == None:
		lib.transmit_mirrorinfo(mymirrorinfo, _global_manifestdict['vendorhostname'], _global_manifestdict['vendorport'])
	else:
		lib.transmit_mirrorinfo(mymirrorinfo, _commandlineoptions.vendorip, _global_manifestdict['vendorport'])


#################### Batch Answer Thread ######################
def BatchAnswer(parallel, chunknumbers, sock):
	global _batchrequests
	global _xorstrings
	global _finish
	global _batch_comp_time

	blocksize = _global_myxordatastore.sizeofblocks
	_batch_comp_time = 0;

	# while a client is connected
	while not _finish:

		# wait for signal to start
		_batchevent.wait()

		# create local copies and reset global values
		with _batchlock:
			batchrequests = _batchrequests
			xorstrings = _xorstrings
			_batchrequests = 0
			_xorstrings = ""

		if batchrequests == 0:
			# all request answered, remove flag and wait/return
			_batchevent.clear()

		else: # answer requests
			start_time = _timer()

			if parallel:
				xoranswer = _global_myxordatastore.produce_xor_from_multiple_bitstrings(xorstrings, batchrequests*len(chunknumbers))
				_batch_comp_time = _batch_comp_time + _timer() - start_time
				i = 0
				for _ in xrange(batchrequests):
					result = {}
					for c in chunknumbers:
						result[c] = xoranswer[i*blocksize : (i+1)*blocksize]
						i = i + 1

					session.sendmessage(sock, msgpack.packb(result))

			else:
				xoranswer = _global_myxordatastore.produce_xor_from_multiple_bitstrings(xorstrings, batchrequests)
				_batch_comp_time = _batch_comp_time + _timer() - start_time
				for i in xrange(batchrequests):
					session.sendmessage(sock, xoranswer[i*blocksize : (i+1)*blocksize])


############################### Serve via RAID-PIR ###############################

# I don't need to change this much, I think...
class ThreadedXORServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	allow_reuse_address = True

class ThreadedXORRequestHandler(SocketServer.BaseRequestHandler):

	def handle(self):

		global _batchrequests
		global _xorstrings
		global _finish
		global _batch_comp_time
		global _global_myxordatastore
		global _global_manifestdict
		global _request_restart

		_finish = False
		comp_time = 0
		_batch_comp_time = 0
		_batchrequests = 0
		_xorstrings = ""
		parallel = False

		requeststring = '0'

		while requeststring != 'Q':
			# read the request from the socket...
			requeststring = session.recvmessage(self.request)

			# for logging purposes, get the remote info
			# remoteip, remoteport = self.request.getpeername()

			start_time = _timer()

			# if it's a request for a XORBLOCK
			if requeststring.startswith('X'):

				bitstring = requeststring[len('X'):]
				expectedbitstringlength = lib.bits_to_bytes(_global_myxordatastore.numberofblocks)

				if len(bitstring) != expectedbitstringlength:
					# Invalid request length...
					#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" Invalid request with length: "+str(len(bitstring)))
					session.sendmessage(self.request, 'Invalid request length')
					_finish = True
					_batchevent.set()
					return

				if not batch:
					# Now let's process this...
					xoranswer = _global_myxordatastore.produce_xor_from_bitstring(bitstring)
					comp_time = comp_time + _timer() - start_time

					# and immediately send the reply.
					session.sendmessage(self.request, xoranswer)
					#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" GOOD")

				else:

					with _batchlock:
						_xorstrings += bitstring
						_batchrequests = _batchrequests + 1

					# notify batch thread
					_batchevent.set()

				# done!

			elif requeststring.startswith('C'):

				payload = requeststring[len('C'):]

				chunks = msgpack.unpackb(payload)

				bitstring = lib.build_bitstring_from_chunks(chunks, k, chunklen, lastchunklen)

				if not batch:
					# Now let's process this...
					xoranswer = _global_myxordatastore.produce_xor_from_bitstring(bitstring)
					comp_time = comp_time + _timer() - start_time

					# and send the reply.
					session.sendmessage(self.request, xoranswer)
					#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" GOOD")

				else:
					with _batchlock:
						_xorstrings += bitstring
						_batchrequests = _batchrequests + 1

					# notify batch thread
					_batchevent.set()

				#done!

			elif requeststring.startswith('R'):

				payload = requeststring[len('R'):]

				chunks = msgpack.unpackb(payload)

				#iterate through r-1 random chunks
				for c in chunknumbers[1:]:

					if c == k - 1:
						length = lastchunklen
					else:
						length = chunklen

					chunks[c] = lib.nextrandombitsAES(cipher, length)


				bitstring = lib.build_bitstring_from_chunks(chunks, k, chunklen, lastchunklen) #the expanded query

				if not batch:
					# Now let's process this...
					xoranswer = _global_myxordatastore.produce_xor_from_bitstring(bitstring)
					comp_time = comp_time + _timer() - start_time

					# and send the reply.
					session.sendmessage(self.request, xoranswer)
					#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" GOOD")

				else:
					with _batchlock:
						_xorstrings += bitstring
						_batchrequests = _batchrequests + 1

					# notify batch thread
					_batchevent.set()

				#done!

			elif requeststring == 'MANIFEST UPDATE':
				print "MANIFEST UPDATE"
				_request_restart = True

			elif requeststring.startswith('M'):
				parallel = True

				payload = requeststring[len('M'):]

				chunks = msgpack.unpackb(payload)

				#iterate through r-1 random chunks
				for c in chunknumbers[1:]:

					if c == k - 1:
						length = lastchunklen
					else:
						length = chunklen

					chunks[c] = lib.nextrandombitsAES(cipher, length)


				bitstrings = lib.build_bitstring_from_chunks_parallel(chunks, k, chunklen, lastchunklen) #the expanded query

				if not batch:

					result = {}
					for c in chunknumbers:
						result[c] = _global_myxordatastore.produce_xor_from_bitstring(bitstrings[c])

					comp_time = comp_time + _timer() - start_time

					# and send the reply.
					session.sendmessage(self.request, msgpack.packb(result))
				else:
					with _batchlock:
						for c in chunknumbers:
							_xorstrings += bitstrings[c]
						_batchrequests = _batchrequests + 1

					# notify batch thread
					_batchevent.set()

				#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" GOOD")
				#done!

			elif requeststring.startswith('P'):

				payload = requeststring[len('P'):]

				params = msgpack.unpackb(payload)

				chunknumbers = params['cn']
				k = params['k']
				r = params['r']
				chunklen = params['cl']
				lastchunklen = params['lcl']
				batch = params['b']
				parallel = params['p']

				if 's' in params:
					cipher = lib.initAES(params['s'])

				if batch:
					# create batch xor thread
					t = threading.Thread(target=BatchAnswer, args=[parallel, chunknumbers, self.request], name="RAID-PIR Batch XOR")
					t.daemon = True
					t.start()

				# and send the reply.
				session.sendmessage(self.request, "PARAMS OK")
				#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" PARAMS received " + str(params))
				#done!

			#Timing Request
			elif requeststring == 'T':
				session.sendmessage(self.request, "T" + str(comp_time + _batch_comp_time))
				comp_time = 0
				_batch_comp_time = 0

			#Debug Hello
			elif requeststring == 'HELLO':
				session.sendmessage(self.request, "HI!")
				#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" HI!")
				# done!

			#the client asked to close the connection
			elif requeststring == 'Q':
				comp_time = 0
				_finish = True
				_batchevent.set()
				return

			#this happens if the client closed the socket unexpectedly
			elif requeststring == '':
				comp_time = 0
				_finish = True
				_batchevent.set()
				return

			else:
				# we don't know what this is!   Log and tell the requestor
				#_log("RAID-PIR "+remoteip+" "+str(remoteport)+" Invalid request type starts:'"+requeststring[:5]+"'")

				session.sendmessage(self.request, 'Invalid request type')
				_finish = True
				_batchevent.set()
				return


def service_raidpir_clients(myxordatastore, ip, port):

	# this should be done before we are called
	assert _global_myxordatastore != None

	# create the handler / server
	xorserver = ThreadedXORServer(('0.0.0.0', port), ThreadedXORRequestHandler)

	# and serve forever!   This call will not return which is why we spawn a new thread to handle it
	t = threading.Thread(target=xorserver.serve_forever, name="RAID-PIR mirror server")
	t.daemon = True
	t.start()

	return xorserver


################################ Serve via HTTP ###############################
import BaseHTTPServer
import urlparse

# handle a HTTP request
class MyHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

	def do_GET(self):

		# get the path part of the request.   Ignore the host name, etc.
		requestedfilename = urlparse.urlparse(self.path).path

		# if there is a leading '/' then kill it.
		if requestedfilename.startswith('/'):
			requestedfilename = requestedfilename[1:]

		# let's look for the file...
		for fileinfo in _global_manifestdict['fileinfolist']:
			# great, let's serve it!
			if requestedfilename == fileinfo['filename']:
				# it's a good query!  Send 200!
				self.send_response(200)
				self.end_headers()

				# and send the response!
				filedata = _global_myxordatastore.get_data(fileinfo['offset'], fileinfo['length'])
				self.wfile.write(filedata)
				return

		# otherwise, it's unknown...
		self.send_error(404)
		return

	# log HTTP information
	def log_message(self, format, *args):
		pass
		#_log("HTTP "+self.client_address[0]+" "+str(self.client_address[1])+" "+(format % args))


def service_http_clients(myxordatastore, manifestdict, ip, port):

	# this must have already been set
	assert _global_myxordatastore != None
	assert _global_manifestdict != None

	httpserver = BaseHTTPServer.HTTPServer((ip, port), MyHTTPRequestHandler)

	# and serve forever! Just like with RAID-PIR, this doesn't return so we need a new thread...
	threading.Thread(target=httpserver.serve_forever, name="HTTP server").start()

########################## Retrieve the manifest dict ##########################
def retrieve_manifest_dict():
	global _commandlineoptions

	# If we were asked to retrieve the mainfest file, do so...
	if _commandlineoptions.retrievemanifestfrom:
		# We need to download this file...
		rawmanifestdata = lib.retrieve_rawmanifest(_commandlineoptions.retrievemanifestfrom)

		# ...make sure it is valid...
		manifestdict = lib.parse_manifest(rawmanifestdata)

		# ...and write it out if it's okay
		open(_commandlineoptions.manifestfilename, "w").write(rawmanifestdata)

	else:
		# Simply read it in from disk
		rawmanifestdata = open(_commandlineoptions.manifestfilename).read()
		manifestdict = lib.parse_manifest(rawmanifestdata)

	return manifestdict

########################### Option parsing and main ###########################
_commandlineoptions = None

def parse_options():
	"""
	<Side Effects>
		All relevant data is added to _commandlineoptions

	<Exceptions>
		These are handled by optparse internally.   I believe it will print / exit
		itself without raising exceptions further.   I do print an error and
		exit if there are extra args...

	<Returns>
		None
	"""
	global _commandlineoptions
	global _logfo

	# should be true unless we're initing twice...
	assert _commandlineoptions == None

	parser = optparse.OptionParser()

	parser.add_option("", "--ip", dest="ip", type="string", metavar="IP",
				default='0', help="Listen for clients on the following IP (default is the public facing IP)")

	parser.add_option("", "--port", dest="port", type="int", metavar="portnum",
				default=62294, help="Use the following port to serve RAID-PIR clients (default 62294)")

	parser.add_option("", "--http", dest="http", action="store_true",
				default=False, help="Serve legacy clients via HTTP (default False)")

	parser.add_option("", "--httpport", dest="httpport", type="int",
				default=80, help="Serve HTTP clients on this port (default 80)")

	parser.add_option("-f", "--files", dest="files", type="string",
				metavar="dir", default=None,
				help="The base directory where all mirror files are located.")

	parser.add_option("-d", "--database", dest="database", metavar="filename", type="string", default=None, help="Read this database file.")

	parser.add_option("", "--retrievemanifestfrom", dest="retrievemanifestfrom",
				type="string", metavar="vendorIP:port", default="",
				help="Specifies the vendor to retrieve the manifest from (default None).")

	parser.add_option("-m", "--manifestfile", dest="manifestfilename",
				type="string", default="manifest.dat",
				help="The manifest file to use (default manifest.dat).")

	parser.add_option("", "--daemon", dest="daemonize", action="store_true",
				default=False,
				help="Detach from terminal and run in the background")

	parser.add_option("", "--logfile", dest="logfilename",
				type="string", default="mirror.log",
				help="The file to write log data to (default mirror.log).")

	parser.add_option("", "--announcedelay", dest="mirrorlistadvertisedelay",
				type="int", default=60,
				help="How many seconds should I wait between vendor notifications? (default 60).")

	parser.add_option("", "--precompute", dest="use_precomputed_data",
				action="store_true", default=False,
				help="Use 4Russian precomputation to speedup PIR responses.")

	parser.add_option("", "--vendorip", dest="vendorip", type="string", metavar="IP",
				default=None, help="Vendor IP for overwriting the value from manifest")

	# let's parse the args
	(_commandlineoptions, remainingargs) = parser.parse_args()

	# check the arguments
	if _commandlineoptions.ip == "0":
		# use external ip, if none is specified
		_commandlineoptions.ip = getmyip.getmyip()

	if _commandlineoptions.port <= 0 or _commandlineoptions.port > 65535:
		print "Specified port number out of range"
		sys.exit(1)

	if _commandlineoptions.httpport <= 0 or _commandlineoptions.httpport > 65535:
		print "Specified HTTP port number out of range"
		sys.exit(1)

	if _commandlineoptions.mirrorlistadvertisedelay < 0:
		print "Mirror advertise delay must be positive"
		sys.exit(1)

	if remainingargs:
		print "Unknown options", remainingargs
		sys.exit(1)

	if not (_commandlineoptions.database == None) ^ (_commandlineoptions.files == None):
		print "Must specify either files or database"
		sys.exit(1)

	# try to open the log file...
	_logfo = open(_commandlineoptions.logfilename, 'a')


def main():
	global _global_myxordatastore
	global _global_manifestdict
	global _batchlock
	global _batchevent
	global _xorstrings
	global _batchrequests
	global _request_restart

	manifestdict = retrieve_manifest_dict()

	# We should detach here.   I don't do it earlier so that error
	# messages are written to the terminal...   I don't do it later so that any
	# threads don't exist already.   If I do put it much later, the code hangs...
	if _commandlineoptions.daemonize:
		daemon.daemonize()

	if _commandlineoptions.database != None:
		print "Using mmap datastore"
		dstype = "mmap"
		source = _commandlineoptions.database
	else:
		print "Using RAM datastore"
		dstype = "RAM"
		source = _commandlineoptions.files

	myxordatastore = fastsimplexordatastore.XORDatastore(manifestdict['blocksize'], manifestdict['blockcount'], dstype, source, _commandlineoptions.use_precomputed_data)

	if dstype == "RAM":
		# now let's put the content in the datastore in preparation to serve it
		print "Loading data into RAM datastore..."
		start = time.clock()
		lib.populate_xordatastore(manifestdict, myxordatastore, source, dstype, _commandlineoptions.use_precomputed_data)
		elapsed = (time.clock() - start)
		print "Datastore initialized. Took %f seconds." % elapsed

	# we're now ready to handle clients!
	#_log('ready to start servers!')

	# an ugly hack, but Python's request handlers don't have an easy way to pass arguments
	_global_myxordatastore = myxordatastore
	_global_manifestdict = manifestdict
	_batchlock = threading.Lock()
	_batchevent = threading.Event()
	_batchrequests = 0
	_xorstrings = ""

	# first, let's fire up the RAID-PIR server
	xorserver = service_raidpir_clients(myxordatastore, _commandlineoptions.ip, _commandlineoptions.port)

	# If I should serve legacy clients via HTTP, let's start that up...
	if _commandlineoptions.http:
		service_http_clients(myxordatastore, manifestdict, _commandlineoptions.ip, _commandlineoptions.httpport)

	#_log('servers started!')
	print "Mirror Server started at", _commandlineoptions.ip, ":", _commandlineoptions.port

	# let's send the mirror information periodically...
	# we should log any errors...
	_send_mirrorinfo()
	counter = 0

	while True:
		if counter > _commandlineoptions.mirrorlistadvertisedelay:
			counter = 0
			try:
				_send_mirrorinfo()
			except Exception, e:
				_log(str(e) + "\n" + str(traceback.format_tb(sys.exc_info()[2])))

		if _request_restart:
			print "Shutting down"
			xorserver.shutdown()
			sys.exit(0)

		counter = counter + 1
		time.sleep(1)

if __name__ == '__main__':
	print "RAID-PIR mirror", lib.pirversion
	parse_options()
	try:
		main()
	except Exception, e:
		# log errors to prevent silent exiting...
		print str(type(e))+" "+str(e)
		# this mess prints a not-so-nice traceback, but it does contain all
		# relevant info
		_log(str(e) + "\n" + str(traceback.format_tb(sys.exc_info()[2])))
		sys.exit(1)
