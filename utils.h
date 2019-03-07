#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include <string>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>

#include "simdcomp.h"

using namespace std;

template<typename T>
T* loadColumn(string col_name, int num_entries) {
  T* h_col = new T[num_entries];

  string filename = col_name;

  ifstream colData (filename.c_str(), ios::in | ios::binary);
  if (!colData) {
    return NULL;
  }

  T number = 0;
  int i = 0;
  while(colData >> number){
    h_col[i++] = number;
  }
  
  return h_col;
}

size_t varying_bit_width_compress(uint32_t * datain, size_t length, uint8_t * buffer) {
  uint8_t * initout;
  size_t k;
  if(length/SIMDBlockSize*SIMDBlockSize != length) {
      printf("Data length should be a multiple of %i \n",SIMDBlockSize);
  }
  initout = buffer;
  for(k = 0; k < length / SIMDBlockSize; ++k) {
    uint32_t b = maxbits(datain);
    *buffer++ = b;
    simdpackwithoutmask(datain, (__m128i *)buffer, b);
    datain += SIMDBlockSize;
    buffer += b * sizeof(__m128i);
  }
  return buffer - initout;
}

/* Here we compress the data in blocks of 128 integers with varying bit width */
uint8_t* encodeColumn(const char* s, int num_elements) {
  size_t N = (num_elements / 128 + 1) * 128; 
  uint32_t* datain = (uint32_t*) malloc(N * sizeof(uint32_t)); 

  // Load in l_shipdate data 
  FILE *fp = fopen(s, "r");

  if(fp == NULL) {
    printf("error in opening file\n");
  }

  size_t i = 0; 
  uint32_t val; 
  while(i < N && (val = fscanf(fp, "%d", &datain[i++])) == 1) ; 

  uint8_t* buffer = (uint8_t*) malloc(N * sizeof(uint32_t) + N / SIMDBlockSize); /* output buffer */

  // Encode 
  size_t compsize = varying_bit_width_compress(datain, N, buffer);
  printf("encoded size: %u (original size: %u)\n", (unsigned)compsize, (unsigned)(N * sizeof(uint32_t)));

  free(datain);
  return buffer; 
}