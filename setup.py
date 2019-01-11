#! /usr/bin/env python

from distutils.core import setup, Extension

import sys

print("This is not intended to be used for any serious purpose.  It is only")
print("constructed to build the C xordatastore.   There will be a serious")
print("version of this written later that covers more of RAID-PIR...")



# Must have Python >=3.5
if sys.version_info[0] != 3 or sys.version_info[1] < 5:
	print("Requires Python >= 3.5")
	sys.exit(1)



fastsimpledatastore_c = Extension("fastsimplexordatastore_c",
		sources=["fastsimplexordatastore.c"]
		#extra_compile_args=["-msse2", "-mstackrealign"] #might be required on some systems
		)

mmapdatastore_c = Extension("mmapxordatastore_c",
		sources=["mmapxordatastore.c"]
		#extra_compile_args=["-msse2", "-mstackrealign"] #might be required on some systems
		)

setup(name="RAID-PIR",
	version="0.9.5",
	ext_modules=[fastsimpledatastore_c, mmapdatastore_c],
	description="""An early version of RAID-PIR with a simple C-based xordatastore.""",
	author="Daniel Demmler",
	author_email="demmler@encrypto.cs.tu-darmstadt.de",
)
