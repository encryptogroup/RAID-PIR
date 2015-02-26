# This module wraps communications in a signaling protocol.	 The purpose is to
# overlay a connection-based protocol with explicit message signaling.
#
# The protocol is to send the size of the message followed by \n and then the
# message itself.	 The size of a message must be able to be stored in
# sessionmaxdigits.	 A size of -1 indicates that this side of the connection
# should be considered closed.

class SessionEOF(Exception):
	pass

# this limits the size of a message to 10**20.	 Should be good enough
sessionmaxdigits = 20

# get the next message off of the socket...
def recvmessage(socketobj):

	messagesizestring = ''
	# first, read the number of characters...
	for _ in xrange(sessionmaxdigits):
		currentbyte = socketobj.recv(1)

		if currentbyte == '\n':
			break

		# not a valid digit
		if currentbyte not in '0123456789' and messagesizestring != '' and currentbyte != '-':
			raise ValueError("Bad message size")

		messagesizestring = messagesizestring + currentbyte

	else:
		# too large
		# raise ValueError, "Bad message size" #TODO check that this is correct
		return ''

	messagesize = int(messagesizestring)

	# nothing to read...
	if messagesize == 0:
		return ''

	# end of messages
	if messagesize == -1:
		raise SessionEOF("Connection Closed")

	if messagesize < 0:
		raise ValueError("Bad message size")

	data = ''
	while len(data) < messagesize:
		chunk =	socketobj.recv(messagesize-len(data))
		if chunk == '':
			raise SessionEOF("Connection Closed")
		data = data + chunk

	return data

# a private helper function
def _sendhelper(socketobj, data):
	sentlength = 0
	# if I'm still missing some, continue to send (I could have used sendall
	# instead but this isn't supported in reply currently)
	while sentlength < len(data):
		thissent = socketobj.send(data[sentlength:])
		sentlength = sentlength + thissent

# send the message
def sendmessage(socketobj, data):
	header = str(len(data)) + '\n'
	_sendhelper(socketobj, header)
	_sendhelper(socketobj, data)
