#  Daemon Module - basic facilities for becoming a daemon process
#
#  Combines ideas from Steinar Knutsens daemonize.py and
#  Jeff Kunces demonize.py

# Originally posted to python-list; an archive of the post is available
# here: http://aspn.activestate.com/ASPN/Mail/Message/python-list/504777
# Assumed is that the author intended for the (fairly trivial body of) code
# to be freely usable by any developer.

"""Facilities for Creating Python Daemons"""

import os
import time
import sys


class NullDevice:
	def write(self, s):
		pass


def daemonize():
	"""
	daemonize:
		Purpose:
			Detach from stdin/stdout/stderr, return control of the term to the user.

		Returns:
			Nothing.

	"""

	if os.name == "nt" or os.name == "ce":
		# No way to fork or daemonize on windows. Just do nothing for now?
		return

	if not os.fork():
		# get our own session and fixup std[in,out,err]
		os.setsid()
		sys.stdin.close()
		sys.stdout = NullDevice()
		sys.stderr = NullDevice()
		if not os.fork():
			# hang around till adopted by init
			ppid = os.getppid()
			while ppid != 1:
				time.sleep(0.5)
				ppid = os.getppid()
		else:
			# time for child to die
			os._exit(0)
	else:
		# wait for child to die and then bail
		os.wait()
		sys.exit()
