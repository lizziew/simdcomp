/* Type "make example" to build this example program. */
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include "simdcomp.h"

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
int compress_l_shipdate() {
    size_t N = 6001280; //6001215 + 65
    uint32_t* datain = malloc(N * sizeof(uint32_t)); 

    // Load in l_shipdate data 
    FILE *fp = fopen("../plain_tpch/data/lineitem/padded_l_shipdate.txt", "r");

    if(fp == NULL) {
        printf("error in opening file\n");
    }

    size_t i = 0; 
    uint32_t val; 
    while(i < N && (val = fscanf(fp, "%d", &datain[i++])) == 1) ; 

    uint8_t* buffer = malloc(N * sizeof(uint32_t) + N / SIMDBlockSize); /* output buffer */
    uint32_t* backbuffer = malloc(N * sizeof(uint32_t));

    // Encode 
    size_t compsize = varying_bit_width_compress(datain, N, buffer);
    printf("encoded size: %u (original size: %u)\n", (unsigned)compsize, (unsigned)(N * sizeof(uint32_t)));

    // Decode
    clock_t start, end;
    start = clock();
    for (size_t k = 0; k * SIMDBlockSize < N; ++k) {
        uint32_t b = *buffer;
        buffer++;
        simdunpack((const __m128i *)buffer, backbuffer + k * SIMDBlockSize, b);
        buffer += b * sizeof(__m128i);
    }
    end = clock();
    double numberofseconds = (end-start)/(double)CLOCKS_PER_SEC;
    printf("decoding took %f ms\n", numberofseconds/1000.0);

    for (size_t k = 0; k < N; ++k) {
        if(backbuffer[k] != datain[k]) {
            printf("bug\n");
            return -1;
        }
    }

    printf("Code works!\n");
    free(datain);
    free(backbuffer);
    return 0;
}

int main() {
    compress_l_shipdate(); 
    return 0;
}