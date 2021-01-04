#ifndef TCP_CLIENT_HH
#define TCP_CLIENT_HH

#include <memory>
#include <thread>

#include "coding_scheme.hh"
#include "sockpp/tcp_connector.h"

class TCPClient {
 protected:
  CodingScheme* c_scheme;
  uint c_node_index;
  sockpp::tcp_connector conn;
  uint conn_num;
  std::unique_ptr<sockpp::tcp_connector[]> node_conns;

  void connect_one(sockpp::inet_address sock_addr, uint index);

 public:
  TCPClient() = default;
  TCPClient(CodingScheme* _scheme, uint i, sockpp::inet_address sock_addr);
  TCPClient(CodingScheme* _scheme, uint i,
            std::vector<sockpp::inet_address>& ia);
  ~TCPClient();
  void send_chunk(uint8_t* data);
  void send_n_chunk(uint8_t** data);
  void send_encode_request();
};

#endif