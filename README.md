# RAID-PIR


RAID-PIR is an efficient implementation of [private information retrieval](https://en.wikipedia.org/wiki/Private_information_retrieval) with multiple servers.

Details of the underlying protocols can be found in the paper "[RAID-PIR: Practical Multi-Server PIR](http://encrypto.de/papers/DHS14.pdf)" published at the [6th ACM Cloud Computing Security Workshop (ACM CCSW'14)](http://digitalpiglet.org/nsac/ccsw14/) by:
* [Daniel Demmler](http://www.ec-spride.tu-darmstadt.de/en/research-groups/engineering-cryptographic-protocols-group/staff/daniel-demmler/), TU Darmstadt, [ENCRYPTO](http://encrypto.de)
* [Amir Herzberg](https://sites.google.com/site/amirherzberg/), Bar Ilan University
* [Thomas Schneider](http://www.thomaschneider.de/), TU Darmstadt, [ENCRYPTO](http://encrypto.de)

This code is an extension of [upPIR](https://uppir.poly.edu) and large parts of it were written by the upPIR maintainers. A big thanks to [Justin Cappos](https://isis.poly.edu/~jcappos/) for making the original upPIR code publicly available.

Please send code-related questions to [Daniel Demmler](mailto:daniel.demmler@ec-spride.de) or create an issue here on github.

**Warning:** This code is **not** meant to be used for a productive environment and is intended for testing and demonstration purposes only.

### Requirements
* Python 2.7
  * [PyCrypto](https://www.dlitz.net/software/pycrypto/) (might require `python-dev` package to build)
  * [MsgPack](http://msgpack.org/)
  * [numpy](http://www.numpy.org/)
* `gcc` (Version 4.x or newer should be fine)
* some sort of somewhat recent Unix (We tested everything on Debian, but MacOS should be OK as well; Windows might work but was never tested...)

## Setting up RAID-PIR Instances and Testing

### 1. Preparations

The following steps describe how to set up instances of an RAID-PIR vendor, mirrors, and client for testing the code locally.

Before you start, make sure you checked out the most recent version from github.

#### 1.1 Fast XOR
To have fast XOR operations, you'll need to build some C code. To do this you have to run `python setup.py build`.

If you cannot get this to work, you can try editing `raidpirlib.py` and `raidpir_mirror.py` to change `import fastsimplexordatastore` to read `import simplexordatastore as fastsimplexordatastore`.

#### 1.2 Directories
Normally each party will run on a separate machine. For testing locally on a single machine, copy and paste the files to different directories for each participant:
e.g. `vendor` where you run the vendor from, `mirror1`, `mirror2`, ..., and `client`.

You can also try to link the files to the different directories for easier editing, but this is **untested and may break things**!

#### 1.3 Setting up the files to be distributed

Now you can copy files over into a directory to be distributed. You can either have a separate directory for each mirror and the vendor (as you would actually have in practice) or share a directory. We'll share a directory called `../files/`. Once the files to share are inside this directory you can create a manifest file.

Command: `python raidpir_create_manifest.py <DIR> <BLOCKSIZE> <VENDOR-IP>`

Example:

```bash
dd@deb:~/workspace/RAID-PIR/test/vendor$ ls ../files/
1.jpg  1.pdf  2.jpg  2.pdf  3.jpg

dd@deb:~/workspace/RAID-PIR/test/vendor$ python raidpir_create_manifest.py ../files/ 4096 127.0.0.1
RAID-PIR create manifest v0.9.0
Fileinfolist generation done.
Indexing done ...
Offset-Dict generated.
Calculating block hashes with algorithm sha256-raw ...
[...]
All blocks done.
Generated manifest.dat describing xordatastore with 326 4096 Byte blocks.
```

### 2. Starting the vendor and mirrors

At this point, We're ready to run the vendor. For testing purposes, it's often useful to run this in the foreground. To do this, use the `--foreground` flag.

Command: `python raidpir_vendor.py --foreground`

Example:

```bash
dd@deb:~/workspace/RAID-PIR/test/vendor$ python raidpir_vendor.py --foreground
RAID-PIR Vendor v0.9.0
Vendor Server started at 127.0.0.1 : 62293
```

In other terminals, you can run mirror instances as well.
Change your terminal to the mirror's directory (such as `../mirror1`).

Each mirror will need to know where to locate the mirror files, what ports to use, and a copy of the manifest file.

Command: `python raidpir_mirror.py --ip <MIRROR-IP> --port <MIRROR-PORT> --foreground --mirrorroot <DIR> --retrievemanifestfrom <VENDOR-IP>`

Example:

```bash
dd@deb:~/workspace/RAID-PIR/test/mirror1$ python raidpir_mirror.py --ip 127.0.0.1 --port 62001 --foreground --mirrorroot ../files/ --retrievemanifestfrom 127.0.0.1
RAID-PIR mirror v0.9.0
Mirror Server started at 127.0.0.1 : 62001
```

We can run another mirror instance in a different terminal. You will need to change to another directory and listen on a different port when you're on a single machine.

```bash
dd@deb:~/workspace/RAID-PIR/test/mirror2$ python raidpir_mirror.py --ip 127.0.0.1 --port 62002 --foreground --mirrorroot ../files/ --retrievemanifestfrom 127.0.0.1
RAID-PIR mirror v0.9.0
Mirror Server started at 127.0.0.1 : 62002
```

Repeat this for the number of mirror servers you want to start. The minimum number of mirror servers required for RAID-PIR (and any other multi-server PIR schemes) is 2.

### 3. Running a RAID-PIR client

Now you can retrieve files using `raidpir_client.py`. Open a terminal in the client directory. First you need the manifest file, which tells you a list of available files and what blocks they map to. The manifest can be requested from the vendor with the same call as the file query.
To retrieve the file `1.jpg`, simply say where to retrieve the manifest from and then the filename to retrieve it.

Command: `python raidpir_client.py [--retrievemanifestfrom <IP:PORT>] <FILENAME> [<FILENAME2> ...]`

Example:
```bash
dd@deb:~/workspace/RAID-PIR/test/client$ python raidpir_client.py --retrievemanifestfrom 127.0.0.1:62293 1.jpg
RAID-PIR Client v0.9.0
Mirrors:  [{'ip': '127.0.0.1', 'port': 62002}, {'ip': '127.0.0.1', 'port': 62003}, {'ip': '127.0.0.1', 'port': 62001}]
Blocks to request: 25
wrote 1.jpg
```

Once you've retrieved the manifest, you can download other files without re-retrieving the manifest (assuming the files and the manifest haven't changed).

#### 3.1 RAID-PIR Optimizations

You can activate several optimizations for the client by specifying command line arguments.
* `-r <number>` activates chunks and sets the redundancy parameter
* `-R` activates randomness expansion from a seed
* `-p` activates parallel multi-block queries

Please see [our RAID-PIR paper](http://encrypto.de/papers/DHS14.pdf) for a detailed explanation of how these optimizations work.

### 4. Restarting the Mirrors or Vendor

You can try to use CTRL-C or CTRL-Z on some OSes to end the RAID-PIR processes. On other OSes, this will raise an exception but will not exit. Also, check your process manager and see if you really terminated the processes. Sometimes this doesn't work due to the multi-threading in RAID-PIR.

A quick and dirty solution is to end all python processes using `killall python` (**Warning:** This will also end all other python processes, not just RAID-PIR!).
In case RAID-PIR still won't terminate properly, try `killall -9 python` (**Warning:** This will most definitely also end all other python processes!). If you know how to solve this more elegantly, please let me know.

You can then re-run the code and your changes will be taken into account.

### 5. General Remarks

All RAID-PIR files that require command line arguments can be called with the argument `-h` or `--help` to display a list of all available options with a short description.

Make sure the ports the vendor and mirror servers are listening on are actually open.

Note that if you update the files to be shared, you will need to re-build the manifest file on the vendor.
