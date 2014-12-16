"""
	 Author: Justin Cappos

	 Start Date: 27 June 2008

	 Description:

	 Adapted from repy's emulcomm (part of the Seattle project)
"""

import socket



STABLE_PUBLIC_IPS = ["18.7.22.69",      # M.I.T
										"171.67.216.8",     # Stanford
										"169.229.131.81",   # Berkley
										"140.142.12.202"]   # Univ. of Washington




def get_localIP_to_remoteIP(connection_type, external_ip, external_port=80):
	"""
	<Purpose>
		Resolve the local ip used when connecting outbound to an external ip.

	<Arguments>
		connection_type:
			The type of connection to attempt. See socket.socket().

		external_ip:
			The external IP to attempt to connect to.

		external_port:
			The port on the remote host to attempt to connect to.

	<Exceptions>
		As with socket.socket(), socketobj.connect(), etc.

	<Returns>
		The locally assigned IP for the connection.
	"""
	# Open a socket
	sockobj = socket.socket(socket.AF_INET, connection_type)

	try:
		sockobj.connect((external_ip, external_port))

		# Get the local connection information for this socket
		(myip, localport) = sockobj.getsockname()

	# Always close the socket
	finally:
		sockobj.close()

	return myip





# Public interface
def getmyip():
	"""
	 <Purpose>
			Provides the external IP of this computer.   Does some clever trickery.

	 <Arguments>
			None

	 <Exceptions>
			As from socket.gethostbyname_ex()

	 <Side Effects>
			None.

	 <Returns>
			The localhost's IP address
			python docs for socket.gethostbyname_ex()
	"""

	# I got some of this from: http://groups.google.com/group/comp.lang.python/browse_thread/thread/d931cdc326d7032b?hl=en
	# however, it has been adapted...

	# Initialize these to None, so we can detect a failure
	myip = None

	# It's possible on some platforms (Windows Mobile) that the IP will be
	# 0.0.0.0 even when I have a public IP and the external IP is up. However, if
	# I get a real connection with SOCK_STREAM, then I should get the real
	# answer.
	for conn_type in [socket.SOCK_DGRAM, socket.SOCK_STREAM]:

		# Try each stable IP
		for ip_addr in STABLE_PUBLIC_IPS:
			try:
				# Try to resolve using the current connection type and
				# stable IP, using port 80 since some platforms panic
				# when given 0 (FreeBSD)
				myip = get_localIP_to_remoteIP(conn_type, ip_addr, 80)
			except (socket.error, socket.timeout):
				# We can ignore any networking related errors, since we want to try
				# the other connection types and IP addresses. If we fail,
				# we will eventually raise an exception anyways.
				pass
			else:
				# Return immediately if the IP address is good
				if myip != None and myip != '' and myip != "0.0.0.0":
					return myip


	# Since we haven't returned yet, we must have failed.
	# Raise an exception, we must not be connected to the internet
	raise Exception("Cannot detect a connection to the Internet.")


if __name__ == '__main__':
	print getmyip()
