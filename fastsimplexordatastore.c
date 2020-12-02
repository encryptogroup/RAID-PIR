/* Author: Daniel Demmler / Justin Cappos
 * File: fastsimplexordatastore.c
 * Purpose: The fastsimplexordatastore.   A simple, C-based datastore
 */

// use Py_ssize_t instead of int for length arguments passed from Python to C
#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include "fastsimplexordatastore.h"

/* I've decided not to mess with making this a Python object.
 * Undoubtably I could do so, but it is harder to understand and verify
 * it doesn't have some sort of bug.   I'll make the C <-> Python portion
 * straightforward and have an intermediate module that makes this
 * more Pythonic.
 */


// This should be *waaaay* more than we would ever need
#define STARTING_XORDATASTORE_TABLESIZE 16

static int xordatastorestablesize = STARTING_XORDATASTORE_TABLESIZE;
static int xordatastoreinited = 0;

static XORDatastore xordatastoretable[STARTING_XORDATASTORE_TABLESIZE];




// Helper
static inline void XOR_fullblocks(__m128i *dest, const __m128i *data, Py_ssize_t count) {
	register long i;
	for (i=0; i<count; i++) {
		*dest = _mm_xor_si128(*data, *dest);
		dest++;
		data++;
	}
}


// Helper
static inline void XOR_byteblocks(char *dest, const char *data, Py_ssize_t count) {
	register long i;
	for (i=0; i<count; i++) {
		*dest++ ^= *data++;
	}
}


// Moves ptr to the next aligned address. If ptr is aligned, return ptr.
static inline char *dword_align(char *ptr) {
	return ptr + (sizeof(__m128i) - (((long)ptr) % sizeof(__m128i))) % sizeof(__m128i);
}


// A helper function that checks to see if the table entry is used or free
static int is_table_entry_used(int i) {
	return (xordatastoretable[i].raw_datastore != NULL);
}


// This allocates memory and stores the size / num_blocks for
// error checking later
static datastore_descriptor allocate(long block_size, long num_blocks)  {
	int i;

	// If it isn't inited, let's fill in the table with empty entries
	if (!xordatastoreinited) {
		// start the table as entry
		for (i=0; i<STARTING_XORDATASTORE_TABLESIZE; i++) {
			xordatastoretable[i].numberofblocks = 0;
			xordatastoretable[i].sizeofablock = 0;
			xordatastoretable[i].raw_datastore = NULL;
			xordatastoretable[i].datastore = NULL;
		}
		// We've initted now!
		xordatastoreinited = 1;
	}

	for (i=0; i<xordatastorestablesize; i++) {
		// Look for an empty entry
		if (!is_table_entry_used(i)) {
			xordatastoretable[i].numberofblocks = num_blocks;
			xordatastoretable[i].sizeofablock = block_size;

			// I allocate a little bit extra so that I can DWORD align it
			xordatastoretable[i].raw_datastore = (char*) calloc(1, num_blocks * block_size + sizeof(__m128i));

			// and align it...
			xordatastoretable[i].datastore = (__m128i *) dword_align(xordatastoretable[i].raw_datastore);
			return i;
		}
	}

	// The table is full! I should expand it...
	printf("Internal Error: I need to expand the table size (unimplemented)\n");
	return -1;
}



// Python wrapper...
static PyObject *Allocate(PyObject *module, PyObject *args) {
	long blocksize, numblocks;

	if (!PyArg_ParseTuple(args, "ll", &blocksize, &numblocks)) {
		// Incorrect args...
		return NULL;
	}

	if (blocksize % 64) {
		PyErr_SetString(PyExc_ValueError, "Block size must be a multiple of 64 byte");
		return NULL;
	}

	return Py_BuildValue("i", allocate(blocksize, numblocks));
}


// This method preprocesses the data using the 4-Russian technique
static inline __m128i* do_preprocessing(long num_blocks, int block_size, long blocks_per_group, char* datastorebase) {
	long num_groups = num_blocks/blocks_per_group;
	long extra_rows = num_blocks%blocks_per_group;

	// the last group may be smaller then all other groups -> extra_rows
	if (extra_rows > 0) {
		num_groups++;
	}

	long group_size = 1<<blocks_per_group;
	int dwords_per_block = block_size / sizeof(__m128i);


	// allocate memory for the current group
	char *raw_precomputation_buffer = (char*) calloc(
		1, block_size*group_size*num_groups + sizeof(__m128i));

	if (raw_precomputation_buffer == NULL) {
		// not enough memory
		printf("Could not allocate memory for precomputation. %ld MBytes needed.\n", block_size * group_size * num_groups / (1024*1024));
		return NULL;
	}

	// align it
	__m128i* precomputation_buffer = (__m128i *) dword_align(
		raw_precomputation_buffer);

	char* datastore_current_group = datastorebase;
	char* current_group = (char*)precomputation_buffer;

	for(long group = 0; group < num_groups; group++) {
		//printf("group %d\n", group);
		unsigned int group_element;

		unsigned int last_graycode = 0;
		unsigned int graycode = 0;
		unsigned int gray_diff = 0;

		// TODO: allocating memory for the first element of the group is not
		//       nesscessary since it will only contain zeros (could save 1/16 of
		//       the allocated memory)
		for (group_element = 1; group_element<group_size; group_element++) {
			last_graycode = graycode;
			graycode = (group_element ^ (group_element>>1));
			gray_diff = graycode ^ last_graycode;

			// offset = (n-1) - log_2(gray_diff)
			// the offset determines the element we would like to XOR. Since the
			// bit_strings are read from left to right, we have to invert
			// log_2(gray_diff)
			long long offset = blocks_per_group-1;
			for(unsigned int i = 1; i < ( (unsigned int) 1<<blocks_per_group); i = i << 1) {
				if (i == gray_diff){
					break;
				}
				offset--;
			}

			// copy the data from the last iteration
			memcpy(current_group + graycode * block_size,
				     current_group + last_graycode * block_size, block_size);

			// XOR the block represented by the change in the graycode
			XOR_fullblocks((__m128i *) (current_group + graycode * block_size),
									   (__m128i *) (datastore_current_group + offset*block_size),
										 dwords_per_block);

			// group element done
		}

		datastore_current_group += blocks_per_group * block_size;
		current_group += group_size * block_size;

		// group done
	}

	// TODO: the original datastore won't be needed any more and could be deleted
	//       to reduce memory consumption since all relevant data is stored in the
	//       precomputation buffer

	return precomputation_buffer;
}


// This function needs to be fast.   It is a good candidate for releasing Python's GIL

static void multi_bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, unsigned int numstrings, __m128i *resultbuffer, char use_precomputed_data) {
	long one_bit_string_length = bit_string_length / numstrings; // length of one bit string
	long remaininglength = one_bit_string_length * 8; // convert bytes to bits

	if (remaininglength > xordatastoretable[ds].numberofblocks){
		remaininglength = xordatastoretable[ds].numberofblocks;
	}

	char *current_bit_string_pos;
	current_bit_string_pos = bit_string;
	long long offset = 0;
	int block_size = xordatastoretable[ds].sizeofablock;
	char *datastorebase;
	datastorebase = (char *) xordatastoretable[ds].datastore;

	int dwords_per_block = block_size / sizeof(__m128i);

	long num_blocks = xordatastoretable[ds].numberofblocks;


	if (use_precomputed_data == 1) {
		long blocks_per_group = 4; // do not change!
		// blocks_per_group is set to a constant number (4) to keep the memory
		// requirements at a manageable level

		__m128i* groups = xordatastoretable[ds].groups;

		if (groups == NULL) {
			printf("Error: xordatastoretable[ds].groups is NULL\n");
			return;
		}

		long group_size = 1<<blocks_per_group;

		long num_groups = num_blocks/blocks_per_group;
		long extra_rows = num_blocks%blocks_per_group;
		// the last group may be smaller then all other groups

		char* current_group = (char*) groups;
		for(long group = 0; group < num_groups; group++) {
			for(unsigned int i = 0; i < numstrings; i++) {
				// this requires blocks_per_group to be 4
				unsigned char current_bitstring_byte =
					*(current_bit_string_pos + one_bit_string_length * i);

				if (group % 2 == 0) {
					offset = ((current_bitstring_byte & 0xf0)>>4);

					if (offset != 0) {
						XOR_fullblocks(resultbuffer + dwords_per_block * i,
												   (__m128i *) (current_group + offset * block_size),
													 dwords_per_block);
					}
				} else {
					offset = (current_bitstring_byte & 0x0f);
					if (offset != 0) {
						XOR_fullblocks(resultbuffer + dwords_per_block * i,
												   (__m128i *) (current_group + offset * block_size),
													 dwords_per_block);
					}
				}
			}
			if (group % 2 == 1) current_bit_string_pos++;
			current_group += block_size * group_size;
		}

		if (extra_rows > 0) {
			long group = num_groups;
			for(unsigned int i = 0; i < numstrings; i++) {
				// this requires blocks_per_group to be 4
				unsigned char current_bitstring_byte =
					*(current_bit_string_pos + one_bit_string_length * i);

				if (group % 2 == 0) {
					offset = ((current_bitstring_byte & 0xf0)>>4);

					if (offset != 0 && offset < (1<<extra_rows)) {
						XOR_fullblocks(resultbuffer + dwords_per_block * i,
													 (__m128i *) (current_group + offset * block_size),
													 dwords_per_block);
					}
				} else {
					offset = (current_bitstring_byte & 0x0f);
					if (offset != 0 && offset < (1<<extra_rows)) {
						XOR_fullblocks(resultbuffer + dwords_per_block * i,
													 (__m128i *) (current_group + offset * block_size),
													 dwords_per_block);
					}
				}
			}
		}


	} else {
		unsigned char bit = 128;
		unsigned int i;

		// let's iterate over all bits of the bit_string
		// each bit of the bit_string represents one PIR block
		while (remaininglength > 0) {

			for(i = 0; i < numstrings; i++){
				if ( *(current_bit_string_pos + one_bit_string_length * i) & bit) {
					XOR_fullblocks(resultbuffer + dwords_per_block * i, (__m128i *) (datastorebase + offset), dwords_per_block);
				}
			}

			offset += block_size;
			bit /= 2;
			remaininglength -=1;
			if (bit == 0) {
				bit = 128;
				current_bit_string_pos++;
			}
		}
	}
}



// This function needs to be fast.   It is a good candidate for releasing Python's GIL

static void bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, __m128i *resultbuffer, char use_precomputed_data) {
	char *current_bit_string_pos = bit_string;
	long long offset = 0;

	int block_size = xordatastoretable[ds].sizeofablock;
	char *datastorebase = (char *) xordatastoretable[ds].datastore;
	long num_blocks = xordatastoretable[ds].numberofblocks;

	int dwords_per_block = block_size / sizeof(__m128i);

	if (use_precomputed_data == 1) {
		long blocks_per_group = 4; // do not change!
		// blocks_per_group is set to a constant number (4) to keep the memory
		// requirements at a manageable level

		__m128i* groups = xordatastoretable[ds].groups;

		if (groups == NULL) {
			printf("Error: xordatastoretable[ds].groups is NULL\n");
			return;
		}

		long group_size = 1<<blocks_per_group;
		long num_groups = num_blocks/blocks_per_group;
		long extra_rows = num_blocks%blocks_per_group;
		// the last group may be smaller then all other groups

		if (extra_rows > 0) {
			num_groups++;
		}

		unsigned char current_bitstring_byte = *(current_bit_string_pos);
		char* current_group = (char*) groups;

		for(long group = 0; group < num_groups; group++) {
			// this requires blocks_per_group to be 4

			if (group % 2 == 0) {
				offset = ((current_bitstring_byte & 0xf0)>>4);
				if (offset != 0) {
					XOR_fullblocks(resultbuffer,
											   (__m128i *) (current_group + offset * block_size),
												 dwords_per_block);
			  }
			} else {
				offset = (current_bitstring_byte & 0x0f);
				if (offset != 0) {
					XOR_fullblocks(resultbuffer,
											   (__m128i *) (current_group + offset * block_size),
												 dwords_per_block);
				}
				current_bit_string_pos++;
				current_bitstring_byte = *(current_bit_string_pos);
			}
			current_group += block_size * group_size;
		}


	} else {
		long remaininglength = bit_string_length * 8;  // convert bytes to bits

		if (remaininglength > xordatastoretable[ds].numberofblocks){
			remaininglength = xordatastoretable[ds].numberofblocks;
		}

		unsigned char bit = 128;

		// let's iterate over all bits of the bit_string
		while (remaininglength > 0) {
			// each bit of the bit_string represents one PIR block
			// if the bit is set, we XOR the block
			if ((*current_bit_string_pos) & bit) {
				XOR_fullblocks(resultbuffer, (__m128i *) (datastorebase + offset), dwords_per_block);
			}
			offset += block_size;
			bit /= 2;
			remaininglength -=1;
			if (bit == 0) {
				bit = 128;
				current_bit_string_pos++;
			}
		}
	}
}






// Does XORs given a bit string. This is the common case and so should be optimized.

// Python Wrapper object
static PyObject *Produce_Xor_From_Bitstring(PyObject *module, PyObject *args) {
	datastore_descriptor ds;
	Py_ssize_t bitstringlength;
	char *bitstringbuffer;
	char *raw_resultbuffer;
	__m128i *resultbuffer;
	char use_precomputed_data;


	if (!PyArg_ParseTuple(args, "iy#b", &ds, &bitstringbuffer, &bitstringlength, &use_precomputed_data)) {
		// Incorrect args...
		return NULL;
	}

	// Is the ds valid?
	if (!is_table_entry_used(ds)) {
		PyErr_SetString(PyExc_ValueError, "Bad index for Produce_Xor_From_Bitstring");
		return NULL;
	}

	// Let's prepare a place to put the answer (1 block + alignment)
	raw_resultbuffer = (char*) calloc(1, xordatastoretable[ds].sizeofablock + sizeof(__m128i));

	// align it
	resultbuffer = (__m128i *) dword_align(raw_resultbuffer);

	// Let's actually calculate this!
	bitstring_xor_worker(ds, bitstringbuffer, bitstringlength, resultbuffer, use_precomputed_data);

	// okay, let's put it in a buffer
	PyObject *return_str_obj = Py_BuildValue("y#", (char *)resultbuffer, xordatastoretable[ds].sizeofablock);

	// clear the buffer
	free(raw_resultbuffer);

	return return_str_obj;
}


// Does XORs given multiple bit strings. This is the common case and so should be optimized.

// Python Wrapper object
static PyObject *Produce_Xor_From_Bitstrings(PyObject *module, PyObject *args) {
	datastore_descriptor ds;
	Py_ssize_t bitstringlength;
	unsigned int numstrings;
	char *bitstringbuffer;
	char *raw_resultbuffer;
	__m128i *resultbuffer;
	char use_precomputed_data;

	if (!PyArg_ParseTuple(args, "iy#Ib", &ds, &bitstringbuffer, &bitstringlength, &numstrings, &use_precomputed_data)) {
		// Incorrect args...
		return NULL;
	}


	// Is the ds valid?
	if (!is_table_entry_used(ds)) {
		PyErr_SetString(PyExc_ValueError, "Bad index for Produce_Xor_From_Bitstring");
		return NULL;
	}

	// Let's prepare a place to put the answer (numstrings blocks + alignment)
	raw_resultbuffer = (char*) calloc(1, xordatastoretable[ds].sizeofablock * numstrings + sizeof(__m128i));

	// align it
	resultbuffer = (__m128i *) dword_align(raw_resultbuffer);

	// Let's actually calculate this!
	multi_bitstring_xor_worker(ds, bitstringbuffer, bitstringlength, numstrings, resultbuffer, use_precomputed_data);

	// okay, let's put it in a buffer
	PyObject *return_str_obj = Py_BuildValue("y#", (char *)resultbuffer, xordatastoretable[ds].sizeofablock * numstrings);

	// clear the buffer
	free(raw_resultbuffer);

	return return_str_obj;
}


// This is used to populate the datastore. It can also be used to add memorization data.

// Python wrapper (only)...
static PyObject *SetData(PyObject *module, PyObject *args) {
	long long offset;
	datastore_descriptor ds;
	char *stringbuffer;
	Py_ssize_t quantity;


	if (!PyArg_ParseTuple(args, "iLy#", &ds, &offset, &stringbuffer, &quantity)) {
		// Incorrect args...
		return NULL;
	}

	// Is the ds valid?
	if (!is_table_entry_used(ds)) {
		printf("ds: %i\n", ds);
		PyErr_SetString(PyExc_ValueError, "Bad index for SetData");
		return NULL;
	}

	// Is this outside of the bounds...
	if (offset + quantity > xordatastoretable[ds].numberofblocks * xordatastoretable[ds].sizeofablock) {
		PyErr_SetString(PyExc_ValueError, "SetData out of bounds");
		return NULL;
	}

	memcpy(((char *)xordatastoretable[ds].datastore)+offset, stringbuffer, quantity);

	return Py_BuildValue("");

}


// Returns the data stored at an offset.   Note that we move away from
// blocks here.   We might as well do the math in Python.   We use this to do
// integrity checking and serve legacy clients.   It is not needed for the
// usual mirror actions.

// Python wrapper (only)...
static PyObject *GetData(PyObject *module, PyObject *args) {
	long long offset, quantity;
	datastore_descriptor ds;

	if (!PyArg_ParseTuple(args, "iLL", &ds, &offset, &quantity)) {
		// Incorrect args...
		return NULL;
	}

	// Is the ds valid?
	if (!is_table_entry_used(ds)) {
		PyErr_SetString(PyExc_ValueError, "Bad index for GetData");
		return NULL;
	}

	// Is this outside of the bounds...
	if (offset + quantity > xordatastoretable[ds].numberofblocks * xordatastoretable[ds].sizeofablock) {
		PyErr_SetString(PyExc_ValueError, "GetData out of bounds");
		return NULL;
	}

	return Py_BuildValue("y#", ((char *)xordatastoretable[ds].datastore)+offset, quantity);
}


// Cleans up the datastore.   I don't know when or why this would be used, but
// it is included for completeness.
static void deallocate(datastore_descriptor ds){
	if (!is_table_entry_used(ds)) {
		printf("Error, double deallocate on %d.   Ignoring.\n",ds);
	}
	else {
		free(xordatastoretable[ds].raw_datastore);
		xordatastoretable[ds].numberofblocks = 0;
		xordatastoretable[ds].sizeofablock = 0;
		xordatastoretable[ds].raw_datastore = NULL;
		xordatastoretable[ds].datastore = NULL;
		// TODO: free raw_precomputation_buffer
	}
}


// Python wrapper...
static PyObject *Deallocate(PyObject *module, PyObject *args) {
	datastore_descriptor ds;

	if (!PyArg_ParseTuple(args, "i", &ds)) {
		// Incorrect args...
		return NULL;
	}

	deallocate(ds);

	return Py_BuildValue("");
}


// Python wrapper...
static PyObject *DoPreprocessing(PyObject *module, PyObject *args) {
	datastore_descriptor ds;

	if (!PyArg_ParseTuple(args, "i", &ds)) {
		// Incorrect args...
		return NULL;
	}

	// Is the ds valid?
	if (!is_table_entry_used(ds)) {
		PyErr_SetString(PyExc_ValueError, "Bad index for Produce_Xor_From_Bitstring");
		return NULL;
	}

	long num_blocks = xordatastoretable[ds].numberofblocks;
	int block_size = xordatastoretable[ds].sizeofablock;

	long blocks_per_group = 4; // do not change!

	char *datastorebase;
	datastorebase = (char *) xordatastoretable[ds].datastore;

	xordatastoretable[ds].groups = do_preprocessing(num_blocks, block_size, blocks_per_group, datastorebase);

	return Py_BuildValue("");
}



// I just have this around for testing
static char *slow_XOR(char *dest, const char *data, Py_ssize_t stringlength) {
	XOR_byteblocks(dest, data, stringlength);
	return dest;
}


// This XORs data with the starting data in dest
static char *fast_XOR(char *dest, const char *data, Py_ssize_t stringlength) {
	int leadingmisalignedbytes;
	long fulllengthblocks;
	int remainingbytes;

	if (stringlength < 1) {
		printf("Error: Bytes to XOR must be at least 1 (and positive)! Got %ld.\n", stringlength);
		return NULL;
	}

	// If it's shorter than a block, use char-based XOR
	if ((size_t)stringlength <= sizeof(__m128i)) {
		return slow_XOR(dest, data, stringlength);
	}


	// I would guess these should be similarly DWORD aligned...
	if (((long) dest) % sizeof(__m128i) != ((long) data) % sizeof(__m128i)) {
		printf("Error, assumed that dest and data are identically DWORD aligned!\n");
		return NULL;
	}

	// Let's XOR any stray bytes at the front...

	// This is the number of bytes that are before we get DWORD aligned
	// To compute this we do (16 - (pos % 16)) % 16)
	leadingmisalignedbytes = (sizeof(__m128i) - (((long)data) % sizeof(__m128i))) % sizeof(__m128i);

	XOR_byteblocks(dest, data, leadingmisalignedbytes);

	// The middle will be done with full sized blocks...
	fulllengthblocks = (stringlength-leadingmisalignedbytes) / sizeof(__m128i);

	XOR_fullblocks((__m128i *) (dest+leadingmisalignedbytes), (__m128i *) (data + leadingmisalignedbytes), fulllengthblocks);


	// XOR anything left over at the end...
	remainingbytes = stringlength - (leadingmisalignedbytes + fulllengthblocks * sizeof(__m128i));
	XOR_byteblocks(dest+stringlength-remainingbytes, data+stringlength-remainingbytes, remainingbytes);

	return dest;

}


// A convenience function for XORing blocks of data together. It is used by
// the client to compute the result and XOR bitstrings
static PyObject *do_xor(PyObject *module, PyObject *args) {
	const char *str1, *str2;
	Py_ssize_t length;
	char *destbuffer;
	char *useddestbuffer;

	// Parse the calling arguments
	if (!PyArg_ParseTuple(args, "y#y#", &str1, &length, &str2, &length)) {
		return NULL;
	}

	// Allocate enough memory to hold the result...
	destbuffer = (char *) malloc(length + sizeof(__m128i));

	if (destbuffer == NULL) {
		PyErr_NoMemory();
		PyErr_SetString(PyExc_MemoryError, "Could not allocate memory for XOR.");
		return NULL;
	}

	// let's align this to str2
	useddestbuffer = destbuffer + ((long) str2 % sizeof(__m128i));

	// ... copy str1 over
	memcpy(useddestbuffer, str1, length);

	// Now, let's do the XOR...
	fast_XOR(useddestbuffer, str2, length);

	// Okay, let's return the answer!
	PyObject *return_str_obj = Py_BuildValue("y#", useddestbuffer, length);

	// (after freeing the buffer)
	free(destbuffer);

	return return_str_obj;

}

static PyMethodDef FastSimpleXORDatastoreMethods [] = {
	{"Allocate", Allocate, METH_VARARGS, "Allocate a datastore."},
	{"Deallocate", Deallocate, METH_VARARGS, "Deallocate a datastore."},
	{"GetData", GetData, METH_VARARGS, "Reads data out of a datastore."},
	{"SetData", SetData, METH_VARARGS, "Puts data into the datastore."},
	{"DoPreprocessing", DoPreprocessing, METH_VARARGS, "Preprocesses the data."},
	{"Produce_Xor_From_Bitstring", Produce_Xor_From_Bitstring, METH_VARARGS, "Extract XOR from datastore."},
	{"Produce_Xor_From_Bitstrings", Produce_Xor_From_Bitstrings, METH_VARARGS, "Extract XORs from datastore."},
	{"do_xor", do_xor, METH_VARARGS, "does the XOR of two equal length strings."},
	{NULL, NULL, 0, NULL}
};


static struct PyModuleDef MyFastSimpleXORDatastoreModule = {
    PyModuleDef_HEAD_INIT,
    "fastsimplexordatastore_c",
    NULL,
    -1,
    FastSimpleXORDatastoreMethods
};

PyMODINIT_FUNC PyInit_fastsimplexordatastore_c(void)
{
    return PyModule_Create(&MyFastSimpleXORDatastoreModule);
}
