/* Author: Daniel Demmler / Justin Cappos
 * Inspired by code from Geremy Condra
 * File: fastsimplexordatastore.h
 * Purpose: The header file for the fastsimplexordatastore
 */

#include "Python.h"
#include <stdint.h>
#include <emmintrin.h>
#include <sys/mman.h>
#include <fcntl.h>

typedef int datastore_descriptor;

typedef struct {
	long numberofblocks;      // Blocks in the datastore
	long sizeofablock;        // Bytes in a block.
	__m128i *datastore;      // This is the DWORD aligned start to the datastore
} XORDatastore;


// Define all of the functions...
static inline void XOR_fullblocks(__m128i *dest, const __m128i *data, long count);
static inline void XOR_byteblocks(char *dest, const char *data, long count);
static inline char *dword_align(char *ptr);
static int is_table_entry_used(int i);
static void bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, __m128i *resultbuffer);
static void multi_bitstring_xor_worker(int ds, char *bit_string, long bit_string_length, unsigned int num_bitstrings, __m128i *resultbuffer);
static void deallocate(datastore_descriptor ds);
static char *slow_XOR(char *dest, const char *data, unsigned long stringlength);
static char *fast_XOR(char *dest, const char *data, unsigned long stringlength);
static PyObject *do_xor(PyObject *module, PyObject *args);
static PyObject *Deallocate(PyObject *module, PyObject *args);
static PyObject *Produce_Xor_From_Bitstring(PyObject *module, PyObject *args);
static PyObject *Produce_Xor_From_Bitstrings(PyObject *module, PyObject *args);
static PyObject *Initialize(PyObject *module, PyObject *args);
static PyObject *GetData(PyObject *module, PyObject *args);
