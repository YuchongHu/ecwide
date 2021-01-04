#include <iostream>

#include "isa-l.h"

using namespace std;

int main() {
  const int k = 16;
  const int m = 2;
  const int n = k + m;

  uint8_t* matrix = new uint8_t[k * n]();
  uint8_t* gftbl = new uint8_t[k * m * 32]();
  uint8_t* encode_matrix = matrix + k * k;

  gf_gen_cauchy1_matrix(matrix, n, k);

  int i, j, index;
  cout << "matrix:\n";
  for (i = index = 0; i < n; ++i) {
    for (j = 0; j < k; ++j) {
      cout << +matrix[index++] << " ";
    }
    cout << "\n";
  }
  cout << "\n";

  ec_init_tables(k, m, encode_matrix, gftbl);

  cout << "gftbl:\n";
  for (i = index = 0; i < k * m; ++i) {
    for (j = 0; j < 32; ++j) {
      cout << +gftbl[index++] << " ";
    }
    cout << "\n";
  }
  cout << "\n";

  uint8_t** data = new uint8_t*[k];
  data[0] = new uint8_t[4]{1, 2, 3, 4};
  data[1] = new uint8_t[4]{58, 96, 45, 12};
  data[2] = new uint8_t[4]{89, 54, 63, 11};
  data[3] = new uint8_t[4]{8, 6, 87, 50};
  uint8_t** parity = new uint8_t*[m];
  parity[0] = new uint8_t[4]();
  parity[1] = new uint8_t[4]();

  ec_encode_data(4, k, m, gftbl, data, parity);

  cout << "parity:\n";
  for (i = 0; i < m; ++i) {
    for (j = 0; j < 4; ++j) {
      cout << +parity[i][j] << " ";
    }
    cout << "\n";
  }
  cout << "\n";

  for (j = 0; j < 4; ++j) {
    uint8_t s = 0;
    for (i = 0; i < k; ++i) {
      s ^= data[i][j];
    }
    cout << +s << " ";
  }
  cout << "\n";

  return 0;
}