#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include <stdio.h>

#include "tbb/tbb.h"
#include "tbb/parallel_for.h"

#include "utils.h"

// #define L_LEN 6001215
// #define N_LEN 25
// #define O_LEN 1500000
// #define C_LEN 150000
// #define S_LEN 10000
// #define R_LEN 5

#define L_LEN 59986052
#define N_LEN 25
#define O_LEN 15000000
#define C_LEN 1500000
#define S_LEN 100000
#define R_LEN 5

using namespace std;
using namespace tbb;

#define BATCH_SIZE 128
#define NUM_THREADS 128

/*
Query 6:
SELECT
  sum(l_extendedprice * l_discount) as revenue
FROM
  lineitem
WHERE
  l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1994-01-01' + interval '1' year
  AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24
*/
float runQuery(float* l_extendedprice, float* l_discount, uint8_t* l_shipdate, uint8_t* l_quantity, bool* selection_flags, int num_items) {
  size_t N = (num_items / 128 + 1) * 128;
  size_t num_batches = (num_items / 128 + 1);
  
  chrono::high_resolution_clock::time_point start, finish;
  start = chrono::high_resolution_clock::now();

  tbb::atomic<unsigned long long> revenue = 0;

  uint32_t* decoded_l_shipdate = (uint32_t*) malloc(N * sizeof(uint32_t));
  uint8_t** shipdate_ptrs = (uint8_t **) malloc(num_batches * sizeof (uint8_t*));
  uint32_t* shipdate_b = (uint32_t*) malloc(num_batches * sizeof(uint32_t));

  uint32_t* decoded_l_quantity = (uint32_t*) malloc(N * sizeof(uint32_t));
  uint8_t** quantity_ptrs = (uint8_t **) malloc(num_batches * sizeof (uint8_t*));
  uint32_t* quantity_b = (uint32_t*) malloc(num_batches * sizeof(uint32_t));

  // Prepare to decode l_shipdate[start] ... l_shipdate[N]
  uint8_t* l_shipdate_copy = l_shipdate;
  #pragma simd
  for (size_t k = 0; k < num_batches; k++) {
    uint32_t b = *l_shipdate_copy;
    l_shipdate_copy++;

    shipdate_ptrs[k] = l_shipdate_copy;
    shipdate_b[k] = b;
    
    l_shipdate_copy += b * sizeof(__m128i);
  }

  // Prepare to decode l_quantity[start] ... l_quantity[N]
  uint8_t* l_quantity_copy = l_quantity; 

  #pragma simd
  for (size_t k = 0; k < num_batches; k++) {
    uint32_t b = *l_quantity_copy;
    l_quantity_copy++;

    quantity_ptrs[k] = l_quantity_copy;
    quantity_b[k] = b;

    l_quantity_copy += b * sizeof(__m128i);
  }

  // Decode l_shipdate and l_quantity
  parallel_for(blocked_range<size_t>(0, N, N/NUM_THREADS + 4), [&](auto range) {
    int start = range.begin();
    int end = range.end();
    int end_batch = start + ((end - start)/BATCH_SIZE) * BATCH_SIZE;

    for (int batch_start = start; batch_start < end_batch; batch_start += BATCH_SIZE) {
      int k = batch_start / 128;
      simdunpack((const __m128i *) shipdate_ptrs[k], decoded_l_shipdate + k * BATCH_SIZE, shipdate_b[k]);
      simdunpack((const __m128i *) quantity_ptrs[k], decoded_l_quantity + k * BATCH_SIZE, quantity_b[k]);
    }

    int k = end_batch / 128;
    simdunpack((const __m128i *) shipdate_ptrs[k], decoded_l_shipdate + k * BATCH_SIZE, shipdate_b[k]);
    simdunpack((const __m128i *) quantity_ptrs[k], decoded_l_quantity + k * BATCH_SIZE, quantity_b[k]);
  }); 
 
  parallel_for(blocked_range<size_t>(0, num_items, num_items/NUM_THREADS + 4), [&](auto range) {
    int start = range.begin();
    int end = range.end();
    int end_batch = start + ((end - start)/BATCH_SIZE) * BATCH_SIZE;
    unsigned long long local_revenue = 0; 

    for (int batch_start = start; batch_start < end_batch; batch_start += BATCH_SIZE) {
      #pragma simd
      for (int i = batch_start; i < batch_start + BATCH_SIZE; i++) {
        selection_flags[i] = (decoded_l_shipdate[i]  > 19940000 && decoded_l_shipdate[i]  < 19950000);
        selection_flags[i] = selection_flags[i] && (decoded_l_quantity[i] < 24);
        selection_flags[i] = selection_flags[i] && (l_discount[i] >= 0.05 && l_discount[i] <= 0.07);
        local_revenue += selection_flags[i] * (l_extendedprice[i] * l_discount[i]);
      }
    }

    for (int i = end_batch; i < end; i++) {
      selection_flags[i] = (decoded_l_shipdate[i]  > 19940000 && decoded_l_shipdate[i]  < 19950000);
      selection_flags[i] = selection_flags[i] && (decoded_l_quantity[i] < 24);
      selection_flags[i] = selection_flags[i] && (l_discount[i] >= 0.05 && l_discount[i] <= 0.07);
      local_revenue += selection_flags[i] * (l_extendedprice[i] * l_discount[i]);
    } 
    revenue.fetch_and_add(local_revenue);
  });

  finish = chrono::high_resolution_clock::now();

  // cout << "Revenue: " << revenue << endl;

  std::chrono::duration<double> diff = finish - start;
  return diff.count() * 1000;
}

int main(int argc, char** argv) {
  int num_trials = 3;

  // Load in data
  chrono::high_resolution_clock::time_point start, finish;
  start = chrono::high_resolution_clock::now();

  float *l_extendedprice = loadColumn<float>("../plain_tpch_10/data/lineitem/l_extendedprice.txt", L_LEN);
  float *l_discount = loadColumn<float>("../plain_tpch_10/data/lineitem/l_discount.txt", L_LEN);
  uint8_t* l_shipdate = encodeColumn("../plain_tpch_10/data/lineitem/padded_l_shipdate.txt", L_LEN);
  uint8_t* l_quantity = encodeColumn("../plain_tpch_10/data/lineitem/padded_l_quantity.txt", L_LEN);

  finish = chrono::high_resolution_clock::now();
  std::chrono::duration<double> diff = finish - start;
  int load_time = diff.count() * 1000;
  
  cout << "Loaded Data: " << load_time << endl;

  // For selection: Initally assume everything is selected
  bool *selection_flags = (bool*) malloc(sizeof(bool) * L_LEN);
  for (size_t i = 0; i < L_LEN; i++) {
    selection_flags[i] = true;
  }
  
  // Run trials
  for (int t = 0; t < num_trials; t++) {
    float time_query = runQuery(l_extendedprice,
                                l_discount,
                                l_shipdate,
                                l_quantity,
                                selection_flags,
                                L_LEN); 

    cout << "{" << "\"query\":6" << ",\"time_query\":" << time_query << "}" << endl;
  }

  return 0;
}
