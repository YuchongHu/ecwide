#ifndef TCP_NODE_HH
#define TCP_NODE_HH

#include "encode_worker.hh"
#include "tcp_client.hh"
#include "tcp_server.hh"

class TCPNode : public TCPServer, public TCPClient {
 private:
  EncodeWorker encode_worker;
  uint8_t** data_buffer;
  uint8_t** parity_buffer;
  uint8_t** intemediate_buffer;
  uint cur_data_num;

  void alloc_memory();
  void do_process();

 public:
  TCPNode(in_port_t _server_port, CodingScheme* _scheme, uint _node_index,
          sockpp::inet_address sock_addr);
  TCPNode(in_port_t _server_port, CodingScheme* _scheme,
          std::vector<sockpp::inet_address>& targets);
  ~TCPNode();

  void run_slave();
  void run_master(uint num);
};

#endif