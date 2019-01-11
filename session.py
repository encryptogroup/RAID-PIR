# This module wraps communications in a signaling protocol.	 The purpose is to
# overlay a connection-based protocol with explicit message signaling.
#
# The protocol is to send the size of the message of length lengthbytes followed by the
# message itself. A size of -1 indicates that this side of the connection
# should be considered closed.

class SessionEOF(Exception):
	pass

# messages are at most 32 bit = 4 bytes long
lengthbytes = 4

# get the next message off of the socket...
def recvmessage(socketobj):

	# receive length of next message
	msglen = socketobj.recv(lengthbytes)
	messagesize = int.from_bytes(msglen, byteorder = 'big', signed=True)

	#print("rcv", messagesize, end="")

	# nothing to read...
	if messagesize == 0:
		return b''

	# end of messages
	if messagesize == -1:
		raise SessionEOF("Connection Closed")

	if messagesize < 0:
		raise ValueError("Bad message size")

	data = b''
	while len(data) < messagesize:
		chunk =	socketobj.recv(messagesize-len(data))
		if chunk == b'':
			raise SessionEOF("Connection Closed")
		data = data + chunk

	#print(":", data[:8])

	return data

# a private helper function
def _sendhelper(socketobj, data):
	#print("send", len(data), ":", str(data[0:8]), "...")
	sentlength = 0
	# if I'm still missing some, continue to send (I could have used sendall
	# instead but this isn't supported in reply currently)
	while sentlength < len(data):
		thissent = socketobj.send(data[sentlength:])
		sentlength = sentlength + thissent

# send the message
def sendmessage(socketobj, data):
	# the length
	_sendhelper(socketobj, len(data).to_bytes(lengthbytes, byteorder = 'big', signed=True))

	if type(data) == str:
		data = str.encode(data)

	#the data
	_sendhelper(socketobj, data)
