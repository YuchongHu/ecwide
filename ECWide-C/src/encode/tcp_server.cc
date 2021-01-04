#include "tcp_server.hh"

TCPServer::TCPServer(in_port_t _port, CodingScheme* _scheme, uint _node_index)
    : node_index(_node_index),
      scheme{_scheme},
      is_head(node_index == _scheme->group_num),
      task_num{0},
      task_end{false} {
  sockpp::socket_initializer sockInit;
  acc = sockpp::tcp_acceptor(_port);
  if (!acc) {
    std::cerr << "Error creating the acceptor: " << acc.last_error_str()
              << std::endl;
    exit(-1);
  }
  // std::cout << "TCPServer init ok\n";
}

void TCPServer::wait_for_connection() {
  // wait for all nodes to be connected
  uint i = 0, serving_num = is_head ? 1 : 2;
  uint client_index;
  while (i < serving_num) {
    sockpp::inet_address peer;
    // Accept a new client connection
    sockpp::tcp_socket sock = acc.accept(&peer);
    if (!sock) {
      std::cerr << "Error accepting incoming connection: "
                << acc.last_error_str() << std::endl;
    } else {
      sock.read_n(&client_index, sizeof(client_index));  // get client index
      if (client_index) {
        last_node_sock = std::move(sock);
        // std::cout << "recv client " << client_index << "\n";
      } else {  // for master node
        master_sock = std::move(sock);
        master_thread = std::thread(&TCPServer::serve_master, this);
        // std::cout << "recv master\n";
      }
      ++i;
    }
  }
}

void TCPServer::serve_master() {
  uint flag = 0;
  // std::cout << "enter serve_master..\n";
  while (true) {
    // std::cout << "wait for master..\n";
    master_sock.read_n(&flag, sizeof(flag));
    std::unique_lock<std::mutex> lck(task_mtx);
    if (flag) {
      ++task_num;
      lck.unlock();
      task_cv.notify_one();
      // std::cout << "get one signal\n";
    } else {
      task_end = true;
      lck.unlock();
      task_cv.notify_one();
      // std::cout << "get shutdown\n";
      break;
    }
  }
  master_sock.close();
  // std::cout << "serve_master close..\n";
}

void TCPServer::recv_chunks(uint8_t** data) {
  for (uint i = 0; i < scheme->global_parity_num; ++i) {
    last_node_sock.read_n(data[i], scheme->chunk_size);
  }
}

TCPServer::~TCPServer() {
  if (!is_head) {
    last_node_sock.close();
    // std::cout << "slave node ~TCPServer\n";
  }
}