#include "tcp_node.hh"

#include <sys/time.h>

// for slaves
TCPNode::TCPNode(in_port_t _server_port, CodingScheme* _scheme,
                 uint _node_index, sockpp::inet_address sock_addr)
    : data_buffer{nullptr},
      parity_buffer{nullptr},
      intemediate_buffer{nullptr},
      TCPNode::TCPServer(_server_port, _scheme, _node_index),
      TCPClient(_scheme, _node_index, sock_addr),
      encode_worker(_scheme, _node_index) {
  // std::cout << "going to alloc_memory\n";
  if (node_index != scheme->group_num) {
    cur_data_num = scheme->group_data_num;
  } else {
    cur_data_num = scheme->k - (node_index - 1) * scheme->group_data_num;
  }
  alloc_memory();
  std::thread accept_thread = std::thread(&TCPNode::wait_for_connection, this);
  accept_thread.join();
}

void TCPNode::alloc_memory() {
  uint i;
  // std::cout << "start alloc\n";

  data_buffer = new uint8_t*[cur_data_num];
  for (i = 0; i < cur_data_num; ++i) {
    data_buffer[i] = new uint8_t[scheme->chunk_size]();
  }
  parity_buffer = new uint8_t*[scheme->total_parity_num];
  for (i = 0; i < scheme->total_parity_num; ++i) {
    parity_buffer[i] = new uint8_t[scheme->chunk_size]();
  }
  intemediate_buffer = new uint8_t*[scheme->global_parity_num];
  for (i = 0; i < scheme->global_parity_num; ++i) {
    intemediate_buffer[i] = new uint8_t[scheme->chunk_size]();
  }
  // std::cout << "alloc ok\n";
}

// for master
TCPNode::TCPNode(in_port_t _server_port, CodingScheme* _scheme,
                 std::vector<sockpp::inet_address>& targets)
    : TCPClient(_scheme, 0, targets), TCPServer() {}

void TCPNode::run_slave() {
  uint tmp = 1;
  master_sock.write_n(&tmp, sizeof(uint));
  while (!task_end || task_num) {
    // std::cout << "++ run_slave: task_num=" << task_num
    //           << ", task_end= " << task_end << "\n";
    if (!task_num) {
      std::unique_lock<std::mutex> lck(task_mtx);
      task_cv.wait(lck, [&] { return task_num || task_end; });
    }
    if (task_num) {
      do_process();
      std::unique_lock<std::mutex> lck(task_mtx);
      --task_num;
    }
  }
  master_thread.join();
  // std::cout << "shutdown ..\n";
  // std::cout << "master_thread ok ..\n";
}

void TCPNode::do_process() {
  struct timeval start_time, end_time;
  double time;
  gettimeofday(&start_time, nullptr);

  // std::cout << "-- start do_process\n";
  // 1. read file
  FILE* f = fopen("test/large_file", "r");
  if (!f) {
    std::cerr << "error when open file\n";
    exit(-1);
  }
  for (uint i = 0; i < cur_data_num; ++i) {
    fread(data_buffer[i], 1, scheme->chunk_size, f);
  }
  fclose(f);

  // std::cout << "-- read ok\n";

  // 2. local encode & recv
  std::thread encode_thread = std::thread(
      [&] { encode_worker.encode_data(data_buffer, parity_buffer); });
  if (!is_head) {
    std::thread recv_thread =
        std::thread(&TCPNode::recv_chunks, this, intemediate_buffer);
    recv_thread.join();
    // std::cout << "-- recv ok\n";
    encode_thread.join();
    // std::cout << "-- encode ok\n";
    // 3. xor intemediate
    encode_worker.xor_data(parity_buffer + 1, intemediate_buffer);
    // std::cout << "-- xor_data ok\n";
  } else {
    encode_thread.join();
    // std::cout << "-- encode ok\n";
  }
  // 4. send intemediate
  if (node_index != 1) {
    send_n_chunk(intemediate_buffer);
    // std::cout << "-- send ok\n";
  }
  gettimeofday(&end_time, nullptr);
  time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
         (end_time.tv_usec - start_time.tv_usec) / 1000.0;
  std::cout << time << "\n";
}

void TCPNode::run_master(uint num) {
  uint i;
  std::thread thr[conn_num + 1];
  for (i = 1; i <= conn_num; ++i) {
    thr[i] = std::thread([&, i] {
      uint tmp;
      node_conns[i].read_n(&tmp, sizeof(uint));
    });
  }
  for (i = 1; i <= conn_num; ++i) {
    thr[i].join();
  }
  uint flag = 1;
  for (uint c = 0; c < num; ++c) {
    for (uint i = conn_num; i >= 1; --i) {
      node_conns[i].write_n(&flag, sizeof(flag));
    }
  }
}

TCPNode::~TCPNode() {
  if (c_node_index) {
    // std::cout << "slave node ~TCPNode  ";
    uint i;
    for (i = 0; i < cur_data_num; ++i) {
      delete[] data_buffer[i];
    }
    for (i = 0; i < scheme->total_parity_num; ++i) {
      delete[] parity_buffer[i];
    }
    for (i = 0; i < scheme->global_parity_num; ++i) {
      delete[] intemediate_buffer[i];
    }
    delete[] data_buffer;
    delete[] parity_buffer;
    delete[] intemediate_buffer;
    // std::cout << "ok!\n";
  }
}
