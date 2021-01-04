#include <sys/time.h>

#include <cstring>

#include "coding_scheme.hh"
#include "isa-l.h"
#include "utils.hh"
using namespace std;

void generate_global_matrix(int k, int m, uint8_t* global_matrix) {
  uint8_t entire_matrix[k * (k + m)];
  gf_gen_cauchy1_matrix(entire_matrix, k + m, k);
  memcpy(global_matrix, entire_matrix + k * k, k * m);
}

void init_local_table(int local_k, uint8_t* gftbl) {
  uint8_t* matrix = new uint8_t[local_k]();
  for (int i = 0; i < local_k; ++i) {
    matrix[i] = 1;
  }
  ec_init_tables(local_k, 1, matrix, gftbl);
  delete[] matrix;
}

int main(int argc, char* argv[]) {
  int num = 0;
  if (argc != 2 || !~sscanf(argv[1], "%d", &num)) {
    cerr << "num is required" << endl;
    exit(-1);
  }
  CodingScheme* scheme = get_scheme_from_file("config/scheme.ini");
  int k = static_cast<int>(scheme->k),
      global_m = static_cast<int>(scheme->global_parity_num),
      chunk_size = static_cast<int>(scheme->chunk_size),
      local_m = static_cast<int>(scheme->group_num),
      local_k = static_cast<int>(scheme->group_data_num);
  uint8_t *global_matrix = new uint8_t[k * global_m],
          *global_gftl = new uint8_t[32 * k * global_m],
          *local_gftl = new uint8_t[32 * local_k];
  uint8_t **data_buffer = new uint8_t *[k],
          **parity_buffer = new uint8_t *[global_m],
          **local_buffer = new uint8_t *[local_m];
  cout << "read config ok\n";

  int i;
  for (i = 0; i < k; ++i) {
    data_buffer[i] = new uint8_t[chunk_size]();
  }
  for (i = 0; i < global_m; ++i) {
    parity_buffer[i] = new uint8_t[chunk_size]();
  }
  for (i = 0; i < local_m; ++i) {
    local_buffer[i] = new uint8_t[chunk_size]();
  }
  cout << "malloc ok\n";

  generate_global_matrix(k, global_m, global_matrix);
  ec_init_tables(k, global_m, global_matrix, global_gftl);
  init_local_table(local_k, local_gftl);

  cout << "start\n";
  struct timeval start_time, end_time;
  double time;

  for (i = 0; i < num; ++i) {
    gettimeofday(&start_time, nullptr);
    // 1. read file
    FILE* f = fopen("test/xlarge_file", "r");
    if (!f) {
      std::cerr << "error when open file\n";
      exit(-1);
    }
    for (int j = 0; j < k; ++j) {
      fread(data_buffer[j], 1, scheme->chunk_size, f);
    }
    fclose(f);

    // 2. encode data for global parity
    ec_encode_data(chunk_size, k, global_m, global_gftl, data_buffer,
                   parity_buffer);

    // 3. encode data for local parity
    for (int j = 0; j < local_m - 1; ++j) {
      ec_encode_data(chunk_size, local_k, 1, local_gftl,
                     data_buffer + j * local_k, local_buffer + j);
    }
    int t = (local_m - 1) * local_k;
    ec_encode_data(chunk_size, k - t, 1, local_gftl, data_buffer + t,
                   local_buffer + local_m - 1);

    gettimeofday(&end_time, nullptr);
    time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
           (end_time.tv_usec - start_time.tv_usec) / 1000.0;
    cout << time << "\n";
  }

  for (i = 0; i < k; ++i) {
    delete[] data_buffer[i];
  }
  for (i = 0; i < global_m; ++i) {
    delete[] parity_buffer[i];
  }
  for (i = 0; i < local_m; ++i) {
    delete[] local_buffer[i];
  }
  delete[] data_buffer;
  delete[] parity_buffer;
  delete[] global_matrix;
  delete[] global_gftl;
  delete[] local_gftl;
  delete[] local_buffer;

  return 0;
}
