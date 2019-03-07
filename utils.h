#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include <string>

#define DATA_DIR "data/"

#define L_LEN 6001215
#define N_LEN 25
#define O_LEN 1500000
#define C_LEN 150000
#define S_LEN 10000
#define R_LEN 5

// #define L_LEN 59986052
// #define N_LEN 25
// #define O_LEN 15000000
// #define C_LEN 1500000
// #define S_LEN 100000
// #define R_LEN 5

using namespace std;

template<typename T>
T* loadColumn(string col_name, int num_entries) {
  T* h_col = new T[num_entries];

  string filename = DATA_DIR + col_name;

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