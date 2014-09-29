RAID-PIR
========

This is the implementation for the [CCSW'14](http://digitalpiglet.org/nsac/ccsw14/) paper "RAID-PIR: Practical Multi-Server PIR".

RAID-PIR is an efficient implementation of [private information retrieval](https://en.wikipedia.org/wiki/Private_information_retrieval) with multiple servers.

**Author**: [Daniel Demmler](http://www.ec-spride.tu-darmstadt.de/en/research-groups/engineering-cryptographic-protocols-group/staff/daniel-demmler/), TU Darmstadt / EC-SPRIDE / [ENCRYPTO](http://encrypto.de)

Our implementation is an extension of [upPIR](https://uppir.poly.edu) and large parts of it were written by the upPIR maintainers. A big thanks to [Justin Cappos](https://isis.poly.edu/~jcappos/) for making the original upPIR code publicly available.

**Our code will be published soon. We estimate a public release in late October 2014.**

### Requirements
Meanwhile you can setup the requirements, if you're planning on working with our implementation:

* Python 2.7
  * [PyCrypto](https://www.dlitz.net/software/pycrypto/) (might require `python-dev` package to build)
  * [MsgPack](http://msgpack.org/)
  * [numpy](http://www.numpy.org/)
* `gcc` (Version 4.2 or newer should be fine)
* some sort of somewhat recent Unix (MacOS should be OK as well; Windows maybe...)
