#ifndef CODING_SCHEME_HH
#define CODING_SCHEME_HH

#include <cstdlib>

class CodingScheme {
 public:
  uint k;
  uint group_data_num;
  uint global_parity_num;
  uint total_parity_num;
  uint group_num;
  uint chunk_size;

  CodingScheme() = default;
  CodingScheme(uint _k, uint _group_data_num, uint _global_parity_num,
               uint _group_num, uint _chunk_size)
      : k{_k},
        group_data_num{_group_data_num},
        global_parity_num{_global_parity_num},
        total_parity_num{_global_parity_num + 1},
        group_num{_group_num},
        chunk_size{_chunk_size} {}
  ~CodingScheme() = default;
};

#endif