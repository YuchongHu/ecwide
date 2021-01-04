#ifndef TCP_SERVER_HH
#define TCP_SERVER_HH

#include <condition_variable>
#include <memory>
#include <thread>

#include "coding_scheme.hh"
#include "sockpp/tcp_acceptor.h"

class TCPServer {
 protected:
  CodingScheme *scheme;
  uint node_index;
  sockpp::tcp_acceptor acc;
  bool is_head;
  std::thread master_thread;
  sockpp::tcp_socket last_node_sock;
  sockpp::tcp_socket master_sock;
  uint task_num;
  bool task_end;
  std::mutex task_mtx;
  std::condition_variable task_cv;

  void serve_master();

 public:
  TCPServer() = default;
  TCPServer(in_port_t _port, CodingScheme *_scheme, uint _node_index);
  ~TCPServer();

  void wait_for_connection();
  void recv_chunks(uint8_t **data);
};

#endif