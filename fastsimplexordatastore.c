/* Author: Daniel Demmler / Justin Cappos
 * File: fastsimplexordatastore.c
 * Purpose: The fastsimplexordatastore.   A simple, C-based datastore
 */

#include "Python.h"
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
static inline void XOR_fullblocks(__m128i *dest, const __m128i *data, long count) {
	register long i;
	for (i=0; i<count; i++) {
		*dest = _mm_xor_si128(*data, *dest);
		dest++;
		data++;
	}
}


// Helper
static inline void XOR_byteblocks(char *dest, const char *data, long count) {
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


// This function needs to be fast.   It is a good candidate for releasing Python's GIL

static void multi_bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, unsigned int numstrings, __m128i *resultbuffer) {
	long one_bit_string_length = bit_string_length / numstrings; // length of one bit string
	long remaininglength = one_bit_string_length * 8; // convert bytes to bits
	char *current_bit_string_pos;
	current_bit_string_pos = bit_string;
	long long offset = 0;
	int block_size = xordatastoretable[ds].sizeofablock;
	char *datastorebase;
	datastorebase = (char *) xordatastoretable[ds].datastore;

	int dwords_per_block = block_size / sizeof(__m128i);

	unsigned char bit = 128;
	unsigned int i;

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



// This function needs to be fast.   It is a good candidate for releasing Python's GIL

static void bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, __m128i *resultbuffer) {
	long remaininglength = bit_string_length * 8;  // convert bytes to bits
	char *current_bit_string_pos;
	current_bit_string_pos = bit_string;
	long long offset = 0;
	int block_size = xordatastoretable[ds].sizeofablock;
	char *datastorebase;
	datastorebase = (char *) xordatastoretable[ds].datastore;

	int dwords_per_block = block_size / sizeof(__m128i);

	unsigned char bit = 128;

	while (remaininglength > 0) {
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


// Does XORs given a bit string. This is the common case and so should be optimized.

// Python Wrapper object
static PyObject *Produce_Xor_From_Bitstring(PyObject *module, PyObject *args) {
	datastore_descriptor ds;
	int bitstringlength;
	char *bitstringbuffer;
	char *raw_resultbuffer;
	__m128i *resultbuffer;

	if (!PyArg_ParseTuple(args, "is#", &ds, &bitstringbuffer, &bitstringlength)) {
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
	bitstring_xor_worker(ds, bitstringbuffer, bitstringlength, resultbuffer);

	// okay, let's put it in a buffer
	PyObject *return_str_obj = Py_BuildValue("s#",(char *)resultbuffer, xordatastoretable[ds].sizeofablock);

	// clear the buffer
	free(raw_resultbuffer);

	return return_str_obj;
}


// Does XORs given multiple bit strings. This is the common case and so should be optimized.

// Python Wrapper object
static PyObject *Produce_Xor_From_Bitstrings(PyObject *module, PyObject *args) {
	datastore_descriptor ds;
	int bitstringlength;
	unsigned int numstrings;
	char *bitstringbuffer;
	char *raw_resultbuffer;
	__m128i *resultbuffer;


	if (!PyArg_ParseTuple(args, "is#I", &ds, &bitstringbuffer, &bitstringlength, &numstrings)) {
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
	multi_bitstring_xor_worker(ds, bitstringbuffer, bitstringlength, numstrings, resultbuffer);

	// okay, let's put it in a buffer
	PyObject *return_str_obj = Py_BuildValue("s#",(char *)resultbuffer, xordatastoretable[ds].sizeofablock * numstrings);

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
	int quantity; // BUG: This probably should be a larger type.


	if (!PyArg_ParseTuple(args, "iLs#", &ds, &offset, &stringbuffer, &quantity)) {
		// Incorrect args...
		return NULL;
	}

	// Is the ds valid?
	if (!is_table_entry_used(ds)) {
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

	return Py_BuildValue("s#", ((char *)xordatastoretable[ds].datastore)+offset, quantity);

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


// I just have this around for testing
static char *slow_XOR(char *dest, const char *data, unsigned long stringlength) {
	XOR_byteblocks(dest, data, stringlength);
	return dest;
}


// This XORs data with the starting data in dest
static char *fast_XOR(char *dest, const char *data, unsigned long stringlength) {
	int leadingmisalignedbytes;
	long fulllengthblocks;
	int remainingbytes;

	// If it's shorter than a block, use char-based XOR
	if (stringlength <= sizeof(__m128i)) {
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
	long length;
	char *destbuffer;
	char *useddestbuffer;

	// Parse the calling arguments
	if (!PyArg_ParseTuple(args, "s#s#", &str1, &length, &str2, &length)) {
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
	PyObject *return_str_obj = Py_BuildValue("s#", useddestbuffer, length);

	// (after freeing the buffer)
	free(destbuffer);

	return return_str_obj;

}


static PyMethodDef MyFastSimpleXORDatastoreMethods [] = {
	{"Allocate", Allocate, METH_VARARGS, "Allocate a datastore."},
	{"Deallocate", Deallocate, METH_VARARGS, "Deallocate a datastore."},
	{"GetData", GetData, METH_VARARGS, "Reads data out of a datastore."},
	{"SetData", SetData, METH_VARARGS, "Puts data into the datastore."},
	{"Produce_Xor_From_Bitstring", Produce_Xor_From_Bitstring, METH_VARARGS, "Extract XOR from datastore."},
	{"Produce_Xor_From_Bitstrings", Produce_Xor_From_Bitstrings, METH_VARARGS, "Extract XORs from datastore."},
	{"do_xor", do_xor, METH_VARARGS, "does the XOR of two equal length strings."},
	{NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initfastsimplexordatastore_c(void) {
	Py_InitModule("fastsimplexordatastore_c", MyFastSimpleXORDatastoreMethods);
}
