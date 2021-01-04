#include "encode_worker.hh"

EncodeWorker::EncodeWorker(CodingScheme* _scheme, uint _node_index)
    : node_index(_node_index),
      data_chunk_num(_scheme->group_data_num),
      real_k(_scheme->k),
      global_parity_num(_scheme->global_parity_num),
      total_parity_num(_scheme->total_parity_num),
      chunk_size(_scheme->chunk_size) {
  if (node_index == _scheme->group_num) {
    data_chunk_num = real_k - (node_index - 1) * _scheme->group_data_num;
  }
  uint t = data_chunk_num * total_parity_num;
  encode_matrix = new uint8_t[t]();
  encode_gftbl = new uint8_t[32 * t]();
  generate_matrix();
  generate_encode_gftbl();
  generate_xor_gftbl();
  // std::cout << "EncodeWorker init ok\n";
}

void EncodeWorker::generate_xor_gftbl() {
  uint8_t matrix[] = {1, 1};
  ec_init_tables(data_chunk_num, global_parity_num, matrix, xor_gftbl);
}

void EncodeWorker::generate_encode_gftbl() {
  ec_init_tables(data_chunk_num, total_parity_num, encode_matrix, encode_gftbl);
}

void EncodeWorker::generate_matrix() {
  uint real_n = real_k + global_parity_num, matrix_len = real_k * real_n;
  uint8_t entire_matrix[matrix_len];
  gf_gen_cauchy1_matrix(entire_matrix, real_n, real_k);
  uint i = 0;
  while (i < data_chunk_num) {
    encode_matrix[i++] = 1;
  }
  for (uint j = real_k * real_k + (node_index - 1) * data_chunk_num;
       j < matrix_len - real_k; j += real_k) {
    for (uint l = 0; l < data_chunk_num; ++l) {
      encode_matrix[i++] = entire_matrix[l + j];
    }
  }
}

void EncodeWorker::encode_data(uint8_t** data, uint8_t** parity) {
  ec_encode_data(chunk_size, data_chunk_num, total_parity_num, encode_gftbl,
                 data, parity);
  // std::cout << "encode_data ok\n";
}

void EncodeWorker::xor_data(uint8_t** source, uint8_t** target) {
  static uint8_t* data_wrapper[2];
  static uint8_t* target_wrapper[1];
  for (uint i = 0; i < global_parity_num; ++i) {
    data_wrapper[0] = source[i];
    target_wrapper[0] = data_wrapper[1] = target[i];
    ec_encode_data(chunk_size, 2, 1, xor_gftbl, data_wrapper, target_wrapper);
    // std::cout << "** xor " << i << " ok\n";
  }
  // std::cout << "xor_data ok\n";
}

EncodeWorker::~EncodeWorker() {
  // std::cout << "~EncodeWorker\n";
  if (node_index) {
    delete[] encode_matrix;
    delete[] encode_gftbl;
  }
}
