#ifndef UTILS_HH
#define UTILS_HH

#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>

#include "coding_scheme.hh"
#include "sockpp/tcp_connector.h"

CodingScheme *get_scheme_from_file(const std::string &src_path);

uint parse_ip_config(const std::string &src_path, in_port_t port,
                     std::vector<sockpp::inet_address> &ia, bool &flag);

#endif