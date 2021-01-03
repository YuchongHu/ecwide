#pragma once

#include "common.hpp"

//calloc when it inits
struct local_encode_st
{
    //encode with the order of gid rid index_tag chunkID
    //here, all chunk are same, so rid and index_tag
    int num;
    uint32_t chunk_id;
    unsigned char *source_data[LN];

    struct local_encode_st *next;
};

struct global_encode_st
{
    //encode with the order of gid rid index_tag chunkID
    int num;
    uint32_t chunk_id;
    unsigned char *source_data[GN];

    struct global_encode_st *next;
};

char *transfer_ustr_to_str(unsigned char *s, uint32_t len);

unsigned char *transfer_str_to_ustr(char *s, uint32_t len);

void show_local(unsigned char *parity);

void show_global(unsigned char **parity);

void show_data(char *data, int len, const char *s);

void show_unsigned_data(unsigned char *data, int len, const char *s);

unsigned char *l_encode(struct local_encode_st *encode);

unsigned char **g_encode(struct global_encode_st *encode);

unsigned char *l_middle(unsigned char **data, int count);

unsigned char *l_decode(unsigned char **data, unsigned char *recovery, int need);