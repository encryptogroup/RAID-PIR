/* Author: Daniel Demmler / Justin Cappos
 * Inspired by code from Geremy Condra
 * File: fastsimplexordatastore.h
 * Purpose: The header file for the fastsimplexordatastore
 */

#include "Python.h"
#include <stdint.h>
#include <emmintrin.h>


typedef int datastore_descriptor;

typedef struct {
	long numberofblocks;  // Blocks in the datastore
	long sizeofablock;    // Bytes in a block.
	char *raw_datastore;  // This points to what malloc returns...
	__m128i *datastore;   // This is the DWORD aligned start to the datastore
	__m128i *groups;      // This is the DWORD aligned start to the precomputed data
} XORDatastore;

// Define all of the functions...

static inline void XOR_fullblocks(__m128i *dest, const __m128i *data, Py_ssize_t count);
static inline void XOR_byteblocks(char *dest, const char *data, Py_ssize_t count);
static inline char *dword_align(char *ptr);
static int is_table_entry_used(int i);
static datastore_descriptor allocate(long block_size, long num_blocks);
static PyObject *Allocate(PyObject *module, PyObject *args);
static inline __m128i* do_preprocessing(long num_blocks, int block_size, long blocks_per_group, char* datastorebase);
static void bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, __m128i *resultbuffer, char use_precomputed_data);
static void multi_bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, unsigned int num_bitstrings, __m128i *resultbuffer, char use_precomputed_data);
static PyObject *Produce_Xor_From_Bitstring(PyObject *module, PyObject *args);
static PyObject *Produce_Xor_From_Bitstrings(PyObject *module, PyObject *args);
static PyObject *SetData(PyObject *module, PyObject *args);
static PyObject *GetData(PyObject *module, PyObject *args);
static void deallocate(datastore_descriptor ds);
static PyObject *Deallocate(PyObject *module, PyObject *args);
static PyObject *DoPreprocessing(PyObject *module, PyObject *args);
static char *slow_XOR(char *dest, const char *data, Py_ssize_t stringlength);
static char *fast_XOR(char *dest, const char *data, Py_ssize_t stringlength);
static PyObject *do_xor(PyObject *module, PyObject *args);
