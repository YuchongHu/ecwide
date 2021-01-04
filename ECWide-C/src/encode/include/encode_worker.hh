#ifndef ENCODE_WORKER_HH
#define ENCODE_WORKER_HH

#include <iostream>
#include <memory>

#include "coding_scheme.hh"
#include "isa-l.h"

class EncodeWorker {
 private:
  uint node_index = 0;
  uint data_chunk_num = 0;
  uint global_parity_num = 0;
  uint total_parity_num = 0;
  uint real_k = 0;
  uint chunk_size = 0;
  uint8_t* encode_matrix;
  uint8_t* encode_gftbl;
  uint8_t xor_gftbl[64];

  void generate_matrix();
  void generate_encode_gftbl();
  void generate_xor_gftbl();

 public:
  EncodeWorker() = default;
  EncodeWorker(CodingScheme* _scheme, uint _node_index);
  ~EncodeWorker();

  void encode_data(uint8_t** data, uint8_t** parity);
  void xor_data(uint8_t** source, uint8_t** target);
};

#endif