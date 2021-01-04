#include "tcp_client.hh"
using namespace std::chrono;

TCPClient::TCPClient(CodingScheme* _scheme, uint i,
                     sockpp::inet_address sock_addr)
    : c_node_index(i), conn_num(1), c_scheme{_scheme}, node_conns{nullptr} {
  if (i != 1) {
    sockpp::socket_initializer sockInit;
    connect_one(sock_addr, 0);
    // std::cout << "TCPClient init ok!\n";
    // } else {
    // std::cout << "node 1 TCPClient init ok!\n";
  }
}

TCPClient::TCPClient(CodingScheme* _scheme, uint i,
                     std::vector<sockpp::inet_address>& ia)
    : c_node_index(i),
      c_scheme{_scheme},
      conn_num(static_cast<uint>(ia.size())),
      node_conns{new sockpp::tcp_connector[ia.size() + 1]} {
  sockpp::socket_initializer sockInit;
  std::thread connect_thr[conn_num + 1];
  uint j;
  for (j = 1; j <= conn_num; ++j) {
    connect_thr[j] = std::thread(&TCPClient::connect_one, this, ia[j - 1], j);
  }
  for (j = 1; j <= conn_num; ++j) {
    connect_thr[j].join();
  }
}

void TCPClient::connect_one(sockpp::inet_address sock_addr, uint index) {
  // std::cout << "enter connect_one\n";
  sockpp::tcp_connector& cur_conn = index ? node_conns[index] : conn;
  // std::cout << "get tcp_connector\n";
  while (!(cur_conn = sockpp::tcp_connector(sock_addr))) {
    std::this_thread::sleep_for(milliseconds(1));
  }
  cur_conn.write_n(&c_node_index, sizeof(c_node_index));
  // std::cout << "connect client ok\n";
}

void TCPClient::send_chunk(uint8_t* data) {
  conn.write_n(data, c_scheme->chunk_size);
}

void TCPClient::send_n_chunk(uint8_t** data) {
  for (uint i = 0; i < c_scheme->global_parity_num; ++i) {
    conn.write_n(data[i], c_scheme->chunk_size);
  }
}

void TCPClient::send_encode_request() {
  uint i;
  uint flag = 1;

  for (i = conn_num; i >= 1; --i) {
    node_conns[i].write_n(&flag, sizeof(uint));
  }
}

TCPClient::~TCPClient() {
  if (!c_node_index) {
    // std::cout << "slave node ~TCPClient...";
    uint flag = 0;
    for (uint i = conn_num; i >= 1; --i) {
      node_conns[i].write_n(&flag, sizeof(uint));
      node_conns[i].close();
    }
    // std::cout << "OK~!\n";
  } else if (c_node_index != 1) {
    conn.close();
  }
}