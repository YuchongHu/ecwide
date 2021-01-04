#include "utils.hh"

CodingScheme *get_scheme_from_file(const std::string &src_path) {
  std::ifstream f(src_path);
  if (!f.is_open()) {
    std::cerr << "open scheme file error\n";
    exit(-1);
  }
  std::string line, name;
  uint value;
  CodingScheme *scheme = new CodingScheme();
  while (!f.eof()) {
    line.clear();
    getline(f, line);
    if (!line.size() || f.fail()) {
      break;
    }
    auto eq_pos = line.find('=');
    if (eq_pos != std::string::npos) {
      name = line.substr(0, eq_pos);
      auto space_pos = name.find(' ');
      if (space_pos != std::string::npos) {
        name.erase(space_pos, name.size());
      }
      if (name == "codeType") {
        continue;
      }
      sscanf(line.c_str() + eq_pos + 1, "%u", &value);
    } else {
      break;
    }

    if (name == "k") {
      scheme->k = value;
    } else if (name == "groupDataNum") {
      scheme->group_data_num = value;
    } else if (name == "globalParityNum") {
      scheme->global_parity_num = value;
      scheme->total_parity_num = value + 1;
    } else if (name == "groupNum") {
      scheme->group_num = value;
    } else if (name == "chunkSizeBits") {
      scheme->chunk_size = 1 << value;
    }
  }
  f.close();
  return scheme;
}

uint parse_ip_config(const std::string &src_path, in_port_t port,
                     std::vector<sockpp::inet_address> &ia, bool &flag) {
  std::ifstream ifs(src_path);
  if (!ifs.is_open()) {
    std::cerr << "open scheme file error\n";
    exit(-1);
  }
  std::string ip;
  ifs >> ip;
  if (ifs.fail()) {
    std::cerr << "empty file!\n";
    exit(-1);
  }
  uint num;
  if (ip == "localhost" || ip == "127.0.0.1") {
    flag = true;
    ifs >> num;
    if (ifs.fail()) {
      std::cerr << "empty locahost num!\n";
      exit(-1);
    }
    std::cout << "localhost config:\n";
    for (uint i = 0; i < num; ++i) {
      auto cur_port = static_cast<uint16_t>(port + i);
      std::cout << ip << ", " << cur_port << "\n";
      ia.emplace_back(ip, cur_port);
    }
  } else {
    flag = false;
    ia.emplace_back(ip, port);
    num = 1;
    while (!ifs.eof()) {
      ifs >> ip;
      if (ifs.fail()) {
        break;
      }
      ia.emplace_back(ip, port);
      ++num;
    }
  }
  ifs.close();
  if (num < 3) {
    std::cerr << "Nodes num should >= 3!\n";
    exit(-1);
  }
  return num - 1;
}