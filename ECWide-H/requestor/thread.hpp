#pragma once

#include "common.hpp"

//for proxy, local and global data
struct send_arg
{
    int connfd;
    int global; //0->local data, 1->global data
    int gid;
    int rid;
    int index_tag;
    uint32_t chunk_id;

    char send[CHUNK_SIZE];
};

//for middle
struct middle_arg
{
    int connfd;
    int global; //0->local middle, 1->global middle
    int gid;
    int rid;
    uint32_t chunk_id;

    unsigned char middle[CHUNK_SIZE];
};

//for update
struct update_arg
{
    int connfd;
    int global; //0->local update, 1->global update
    int gid;
    int rid;
    uint32_t chunk_id;

    unsigned char update[CHUNK_SIZE];
};

//update ack
struct update_ack_arg
{
    int connfd;
    int global; //0->local update, 1->global update
    int gid;
    int rid;
};

struct gather_arg
{
    int connfd;
    int rid;
    uint32_t chunk_id;
};

//for normal set/get
struct kv_arg
{
    int count;
    int fd;
    char *kv;
};

//for repair KV
struct local_repair_arg
{
    //latency
    struct timeval begin;
    struct timeval end;

    //failed memcached in this proxy
    char key[100];
    int need;
    uint32_t index_tag;
    uint32_t chunk_id; //mark the repair unit
    uint32_t offset;   //pick the value from data chunk
    uint32_t length;

    //each rack give a data chunk
    //same xor middle results from other rack and the left data in this rack
    unsigned char *left_data[RACK - 1 + NODE - 1];
    unsigned char *recovery_data; //only one error

    struct local_repair_arg *next;
};

//for local get repair in server
struct get_reapir_st
{
    char key[100];
    char *value;
};

struct job
{
    void *(*callback_function)(void *arg);
    void *arg;
    struct job *next;
};

struct threadpool
{
    int thread_num;
    int queue_max_num;

    //job queue
    struct job *head;
    struct job *tail;

    pthread_t *pthreads;
    pthread_mutex_t mutex;
    pthread_cond_t queue_empty;
    pthread_cond_t queue_not_empty;
    pthread_cond_t queue_not_full;

    int queue_cur_num; //current task
    int queue_close;
    int pool_close;
};

struct threadpool *threadpool_init(int thread_num, int queue_max_num);
int threadpool_add_job(struct threadpool *pool, void *(*callback_function)(void *arg), void *arg);
int threadpool_destroy(struct threadpool *pool);