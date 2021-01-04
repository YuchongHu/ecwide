#include "tcp_node.hh"
#include "utils.hh"
using namespace std;

#define DEFAULT_PORT 20000
#define TASK_NUM 10

void print_usage() { cerr << "usage: ./node_main  [node_index]" << endl; }

int main(int argc, char* argv[]) {
  uint node_index = 0;
  if (argc != 2 || !~sscanf(argv[1], "%u", &node_index)) {
    print_usage();
    exit(-1);
  }
  // cout << "node_index = " << node_index << "\n";

  // get config
  vector<sockpp::inet_address> ips;
  bool is_localhost = false;
  uint slave_num =
      parse_ip_config("config/nodes.ini", DEFAULT_PORT, ips, is_localhost);
  CodingScheme* scheme = get_scheme_from_file("config/scheme.ini");
  // cout << "slave_num=" << slave_num << ", ips.size()=" << ips.size() << "\n";

  // cout << "k = " << scheme->k << "\ngroup_data_num = " <<
  // scheme->group_data_num
  //      << "\nglobal_parity_num = " << scheme->global_parity_num
  //      << "\ntotal_parity_num = " << scheme->total_parity_num
  //      << "\ngroup_num = " << scheme->group_num
  //      << "\nchunk_size = " << scheme->chunk_size << "\n";

  if (!node_index) {
    ips.erase(ips.begin());
    TCPNode node(DEFAULT_PORT, scheme, ips);
    node.run_master(TASK_NUM);
  } else {
    // cout << "alloc node:\n";
    uint16_t actual_port =
        is_localhost ? DEFAULT_PORT + static_cast<uint16_t>(node_index)
                     : DEFAULT_PORT;
    // cout << "actual_port=" << actual_port << "\n";
    if (node_index < ips.size()) {
      TCPNode node(actual_port, scheme, node_index, ips[node_index - 1]);
      // cout << "run_slave\n";
      node.run_slave();
      // cout << "run_slave ok!\n";
    }
  }
  // cout << "all ok!\n";
  delete scheme;
  exit(0);
  return 0;
}
