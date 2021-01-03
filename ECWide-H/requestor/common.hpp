#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <assert.h>
#include <stdarg.h>
#include <sys/time.h>

#include <isa-l.h>
#include <libmemcached/memcached.h>

#define GROUP 2
#define RACK 2
//index_tag max
#define NODE 1

//local K = rid*index_tag, -1 for local
#define LK (RACK * NODE - 1)
#define LN (LK + 1)

//global K, (group-1) *LK
#define GK ((GROUP - 1) * (RACK * NODE - 1))
#define GN (GK + NODE - 1)

#define COMMAND_SIZE 100
// #define BUFFER_SIZE 4096
#define LISTEN_NUM 200

//server
#define S_IP "192.168.1.79"
#define S_PORT 12000
#define P_PORT 20001

//encode=2, repair=3, update=4
#define LEVEL 1 //high -> less

//need l LEVEL, can show the
void vlog(int l, const char *fmt, ...);
#define VERBOSE(l, fmt, arg...) vlog((int)l, fmt, ##arg);

void print_err(const char *str, int err_no);

int offset(int g, int r, int index_tag, int gid_self);

double timeval_diff(struct timeval *start, struct timeval *end);
