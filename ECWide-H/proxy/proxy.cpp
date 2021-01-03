#include "common.hpp"
#include "thread.hpp"
#include "encode.hpp"

//int P_PORT[GROUP][RACK] = {{12001, 12002}, {12003, 12004}, {12005, 12006}};
//int P_PORT[GROUP][RACK];
char ips[GROUP][RACK][100];

enum status
{
    conn_wait,
    conn_init,
    conn_write,
    conn_read,
    conn_close
};

//int connfd_tmp[100];
int gid_self = 0;
int rid_self = 0;

//for this node, write and read
int connfd_list_W[GROUP][RACK] = {0};

pthread_mutex_t send_mutex = PTHREAD_MUTEX_INITIALIZER;

int connfd_list_R[GROUP][RACK] = {0};
long rack_update[GROUP][RACK] = {0}; //count update in each rack
//log the less update data chunk in libmemcached

int connfd_server;

memcached_return rc;
struct ECHash_st *ech;
struct threadpool *send_pool, *repair_pool, *gather_pool, *middle_pool, *update_pool, *update_ack_pool;

//critical variable
int local_can_encode = 0;
int local_wait_num = 0;
struct local_encode_st *local_encode_list = NULL;
pthread_mutex_t local_encode_mutex;
pthread_cond_t local_encode_cond;

struct timeval l_this_update_begin, l_this_update_end;
struct timeval l_other_update_begin, l_other_update_end;
struct timeval g_update_begin, g_update_end;

static void show_local_encode()
{
    struct local_encode_st *p = local_encode_list;

    if (p == NULL)
        VERBOSE(0, "\nEMPTY ENCODE LIST\n");
    VERBOSE(2, "\nencode_list:\n");
    int i = 0;
    while (p)
    {
        VERBOSE(2, "%d: chunk_id=%u, num=%d\n", i, p->chunk_id, p->num);
        p = p->next;
    }
}

//encoding buffer, when it is full,  pop for local parity
//sorted with gid, rid, index_tag, data chunkID
//in this local parity, we encode all same data chunkID
void *local_encode(void *arg)
{
    pthread_mutex_init(&local_encode_mutex, NULL);
    pthread_cond_init(&local_encode_cond, NULL);

    while (1)
    {
        pthread_mutex_lock(&local_encode_mutex);

        if (local_can_encode == 0)
        {
            VERBOSE(2, "\nChoking in local_encode\n");
            pthread_cond_wait(&local_encode_cond, &local_encode_mutex);
        }

        VERBOSE(2, "\nResuming in local_encode\n");
        struct local_encode_st *p = local_encode_list, *q = p;

        //find can encode_st
        if (p == NULL)
        {
            VERBOSE(2, "\nEncode_list == NULL\n");
        }

        if (p->num == LK)
        {
            local_encode_list = p->next;
        }
        else
        {
            p = p->next;
            while (p)
            {
                if (p->num == LK)
                {
                    q->next = p->next;
                    break;
                }
                q = p;
                p = p->next;
            }
        }

        local_wait_num--;
        local_can_encode--;

        //show_local_encode();
        pthread_mutex_unlock(&local_encode_mutex);

        //normal set memcached
        char key_local[100] = {0};
        sprintf(key_local, "local-%d-%d", gid_self, p->chunk_id);

        // encode P, free in local_encode
        unsigned char *parity = l_encode(p);

        char *pp = transfer_ustr_to_str(parity, CHUNK_SIZE);
        rc = memcached_set(ech->ring, key_local, strlen(key_local), pp, CHUNK_SIZE, 0, 0);
        free(pp);
        if (rc == MEMCACHED_SUCCESS)
        {
            VERBOSE(2, "\nLocal parity %s STORE OK\n", key_local);
        }
        else
        {
            VERBOSE(2, "\nLocal parity %s STORE NOK\n", key_local);
        }

        for (int i = 0; i < LN; i++)
        {
            free(p->source_data[i]);
        }
        free(p);
    }
}

int global_can_encode = 0;
int global_wait_num = 0;
struct global_encode_st *global_encode_list = NULL;
pthread_mutex_t global_encode_mutex;
pthread_cond_t global_encode_cond;

void *global_encode(void *arg)
{
    pthread_mutex_init(&global_encode_mutex, NULL);
    pthread_cond_init(&global_encode_cond, NULL);

    while (1)
    {
        pthread_mutex_lock(&global_encode_mutex);

        if (global_can_encode == 0)
        {
            VERBOSE(2, "\nChoking in global_encode\n");
            pthread_cond_wait(&global_encode_cond, &global_encode_mutex);
        }

        VERBOSE(2, "\nResuming in global_encode\n");
        struct global_encode_st *p = global_encode_list, *q = p;

        //find can encode_st
        if (p == NULL)
        {
            VERBOSE(2, "\nEncode_list == NULL\n");
        }
        if (p->num == GK)
        {
            global_encode_list = p->next;
        }
        else
        {
            p = p->next;
            while (p)
            {
                if (p->num == GK)
                {
                    q->next = p->next;
                    break;
                }
                q = p;
                p = p->next;
            }
        }

        global_wait_num--;
        global_can_encode--;

        pthread_mutex_unlock(&global_encode_mutex);

        //normal set memcached
        char key_global[GN - GK][100];
        for (int i = 0; i < GN - GK; i++)
            memset(key_global[i], 0, 100);
        uint32_t chunk_id = p->chunk_id;
        //for global
        for (int i = 0; i < GN - GK; i++)
            sprintf(key_global[i], "global-%d[%d]", chunk_id, i);

        //encode P, free in local_encode
        unsigned char **parity = g_encode(p);

        VERBOSE(2, "\n\nGlobal parity of chunk_id=%d\n\n", chunk_id);

        for (int i = 0; i < GN - GK; i++)
        {
            char *pp = transfer_ustr_to_str(parity[i], CHUNK_SIZE);
            rc = memcached_set(ech->ring, key_global[i], strlen(key_global[i]), pp, CHUNK_SIZE, 0, 0);
            free(pp);
            if (rc == MEMCACHED_SUCCESS)
            {
                VERBOSE(2, "Global parity %s STORE OK\n", key_global[i]);
            }
            else
            {
                VERBOSE(2, "Global parity %s STORE NOK\n", key_global[i]);
            }
        }

        for (int i = 0; i < GN; i++)
        {
            free(p->source_data[i]);
        }
        free(p);
    }
}

//only send
//for two parts, command and real data
void *proxy_send(void *send_arg)
{
    //char receive_buf[100];
    //format: local/global gid rid index_tag chunk_id
    struct send_arg *tmp = (struct send_arg *)send_arg;
    int fd = tmp->connfd;
    int g, r;
    for (int i = 0; i < GROUP; i++)
    {
        for (int j = 0; j < RACK; j++)
        {
            if (fd == connfd_list_W[i][j])
            {
                g = i;
                r = j;
                break;
            }
        }
    }

    //send local first
    char send_com[COMMAND_SIZE];
    memset(send_com, 0, COMMAND_SIZE);
    //send real value
    char send_buf[CHUNK_SIZE];
    memset(send_buf, 0, CHUNK_SIZE);

    //local
    if (tmp->global == 0)
    {
        sprintf(send_com, "%s %d %d %d %u", "l-encode", tmp->gid, tmp->rid, tmp->index_tag, tmp->chunk_id);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(2, "\n\t****(Local data SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }

        memcpy(send_buf, tmp->send, CHUNK_SIZE);
        ret = send(fd, send_buf, CHUNK_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(2, "\n\t****(Local data SEND real) to (%d,%d)\n", g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }
    else
    {
        sprintf(send_com, "%s %d %d %d %u", "g-encode", tmp->gid, tmp->rid, tmp->index_tag, tmp->chunk_id);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(2, "\n\t####(Global data SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }

        memcpy(send_buf, tmp->send, CHUNK_SIZE);
        ret = send(fd, send_buf, CHUNK_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(2, "\n\t####(Global data SEND real) to (%d,%d)\n", g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }

    free(tmp);
    return NULL;
}

void *middle_send(void *middle_arg)
{
    //format: local/global gid rid chunk_id
    struct middle_arg *tmp = (struct middle_arg *)middle_arg;
    int fd = tmp->connfd;
    int g, r;
    for (int i = 0; i < GROUP; i++)
    {
        for (int j = 0; j < RACK; j++)
        {
            if (fd == connfd_list_W[i][j])
            {
                g = i;
                r = j;
                break;
            }
        }
    }

    //send local first
    char send_com[COMMAND_SIZE];
    memset(send_com, 0, COMMAND_SIZE);
    //send real value
    unsigned char send_buf[CHUNK_SIZE];
    memset(send_buf, 0, CHUNK_SIZE);

    //local
    if (tmp->global == 0)
    {
        sprintf(send_com, "%s %d %d %u", "l-middle", tmp->gid, tmp->rid, tmp->chunk_id);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(3, "\n\t****(Local middle SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }

        memcpy(send_buf, tmp->middle, CHUNK_SIZE);
        ret = send(fd, send_buf, CHUNK_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(3, "\n\t****(Local middle SEND real) to (%d,%d)\n", g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }
    else
    {
        // sprintf(send_com, "%s %d %d %u", "g-middle", tmp->gid, tmp->rid, tmp->chunk_id);

        // int ret = send(fd, send_com, COMMAND_SIZE, 0);
        // if (-1 == ret)
        //     print_err("send failed", errno);
        // else
        // {
        //     VERBOSE(3, "\n\t####(Global Middle SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        // }

        // memcpy(send_buf, tmp->send, CHUNK_SIZE);
        // ret = send(fd, send_buf, CHUNK_SIZE, 0);
        // if (-1 == ret)
        //     print_err("send failed", errno);
        // else
        // {
        //     VERBOSE(3, "\n\t####(Global Middle SEND real) to (%d,%d)\n", g, r);
        // }
    }

    free(tmp);
    return NULL;
}

void *update_send(void *update_arg)
{
    //format: local/global gid rid chunk_id
    struct update_arg *tmp = (struct update_arg *)update_arg;
    int fd = tmp->connfd;
    int g, r;
    for (int i = 0; i < GROUP; i++)
    {
        for (int j = 0; j < RACK; j++)
        {
            if (fd == connfd_list_W[i][j])
            {
                g = i;
                r = j;
                break;
            }
        }
    }

    //send local first
    char send_com[COMMAND_SIZE];
    memset(send_com, 0, COMMAND_SIZE);
    //send real value
    unsigned char send_buf[CHUNK_SIZE];
    memset(send_buf, 0, CHUNK_SIZE);

    //local
    if (tmp->global == 0)
    {
        sprintf(send_com, "%s %d %d %u", "l-update", tmp->gid, tmp->rid, tmp->chunk_id);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(4, "\n\t****(Local update SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }

        memcpy(send_buf, tmp->update, CHUNK_SIZE);
        ret = send(fd, send_buf, CHUNK_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            //show_unsigned_data(send_buf,CHUNK_SIZE,"aaa");
            VERBOSE(4, "\n\t****(Local update SEND real) to (%d,%d)\n", g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }
    else
    {
        sprintf(send_com, "%s %d %d %u", "g-update", tmp->gid, tmp->rid, tmp->chunk_id);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(4, "\n\t####(Global update SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }

        memcpy(send_buf, tmp->update, CHUNK_SIZE);
        ret = send(fd, send_buf, CHUNK_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(4, "\n\t####(Global update SEND real) to (%d,%d)\n", g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }
    free(tmp);
    return NULL;
}

void *update_ack_send(void *update_ack_arg)
{
    struct update_ack_arg *tmp = (struct update_ack_arg *)update_ack_arg;
    int fd = tmp->connfd;
    int g, r;
    for (int i = 0; i < GROUP; i++)
    {
        for (int j = 0; j < RACK; j++)
        {
            if (fd == connfd_list_W[i][j])
            {
                g = i;
                r = j;
                break;
            }
        }
    }

    //send local first
    char send_com[COMMAND_SIZE];
    memset(send_com, 0, COMMAND_SIZE);

    //local
    if (tmp->global == 0)
    {
        sprintf(send_com, "%s %d %d", "ack-l-update", tmp->gid, tmp->rid);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(4, "\n\t****(Local update ack SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }
    else
    {
        sprintf(send_com, "%s %d %d", "ack-g-update", tmp->gid, tmp->rid);

        pthread_mutex_lock(&send_mutex);
        int ret = send(fd, send_com, COMMAND_SIZE, 0);
        if (-1 == ret)
            print_err("send failed", errno);
        else
        {
            VERBOSE(4, "\n\t####(Global update ack SEND command) (%s) to (%d,%d)\n", send_com, g, r);
        }
        pthread_mutex_unlock(&send_mutex);
    }

    free(tmp);
    return NULL;
}

//gather all data chunk and local parity in this rack, and send to failed proxy
void *gather_middle(void *gather_arg)
{
    struct gather_arg *tmp = (struct gather_arg *)gather_arg;
    uint32_t chunk_id = tmp->chunk_id;

    // int total=NODE;
    // if(chunk_id % RACK == rid_self) //local parity rack
    //     total=NODE-1;

    unsigned char **data = (unsigned char **)calloc(NODE + 1, sizeof(unsigned char *));
    for (int i = 0; i < NODE + 1; i++)
        data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));

    int count = 0;

    for (int i = 0; i < NODE; i++)
    {
        if (ech->chunk_list[i][chunk_id].stat == Sealed) //have chunkID in this rack
        {
            char *buffer = gather(ech, i, chunk_id);

            show_data(buffer, CHUNK_SIZE, "Middle data");
            //same gid, same rack, diff index_tag
            //xor, do not consider order
            if (buffer) //may local, or failed
            {
                VERBOSE(3, "\t\tIn other rack, (%u,%u) is ok\n", i, chunk_id);
                unsigned char *pp = transfer_str_to_ustr(buffer, CHUNK_SIZE);
                memcpy(data[count++], pp, CHUNK_SIZE);
                free(pp);
                free(buffer);
            }
        }
        else
        {
            //fill it
            memset(data[count++], 's', CHUNK_SIZE);
            printf("In other rack, (%u,%u) is not Sealed, but is filled\n", i, chunk_id);
        }
    }

    //local in
    if (chunk_id % RACK == rid_self)
    {
        size_t value_length;
        uint32_t flags;
        memcached_return_t rc;

        char key_local[100] = {0};
        sprintf(key_local, "local-%d-%d", gid_self, chunk_id);
        printf("\t\tIn other rack, local parity{%s} is ok\n", key_local);
        char *local = memcached_get(ech->ring, key_local, strlen(key_local), &value_length, &flags, &rc);
        if (local)
        {
            unsigned char *pp = transfer_str_to_ustr(local, CHUNK_SIZE);
            show_local(pp);
            memcpy(data[count++], pp, CHUNK_SIZE);
            free(pp);
        }
        else
        {
            memset(data[count++], 'f', CHUNK_SIZE);
            VERBOSE(3, "\tIn other rack, local parity{%s} is nok, maybe not encoded, but is filled\n", key_local);
        }
    }

    printf("count=%u, NODE=%u\n", count, NODE);

    //if (count == NODE) //must be in, count ==NODE
    //{
        VERBOSE(3, "Middle is ready\n");
        unsigned char *middle = l_middle(data, NODE);
        //middle pool
        struct middle_arg *lm = (struct middle_arg *)calloc(1, sizeof(struct middle_arg));
        lm->connfd = tmp->connfd;
        lm->global = 0;
        lm->gid = gid_self;
        lm->rid = rid_self;
        lm->chunk_id = chunk_id;
        memcpy(lm->middle, middle, CHUNK_SIZE);

        threadpool_add_job(middle_pool, middle_send, lm);
        VERBOSE(3, "\n\t\t$$$$Add [Local middle] task to (%d,%d), chunk_id=%d\n", chunk_id % GROUP, tmp->rid, chunk_id);
    //}

    for (int i = 0; i < NODE + 1; i++)
    {
        free(data[i]);
    }
    free(data);
}

int local_can_repair = 0;
int local_repair_wait_num = 0;
struct local_repair_arg *local_repair_list = NULL;
pthread_mutex_t local_repair_mutex;
pthread_cond_t local_repair_cond;

//repair chunk ==>degraded read
void *repair_chunk(void *local_repair_arg)
{
    struct local_repair_arg *tmp = (struct local_repair_arg *)local_repair_arg;

    VERBOSE(3, "\n\t$$$$Add [Repair KV] task kv in index_tag=%u, chunk_id=%u, offset=%u, length=%u\n", tmp->index_tag, tmp->chunk_id, tmp->offset, tmp->length);

    //all = RACK-1+NODE-1
    //this rack's data chunk
    for (int i = 0; i < NODE; i++)
    {
        if (i == tmp->index_tag)
            continue;

        if (ech->chunk_list[i][tmp->chunk_id].stat == Sealed) //have chunkID in this rack
        {
            char *buffer = gather(ech, i, tmp->chunk_id);

            show_data(buffer, CHUNK_SIZE, "Reapair data");
            //same gid, same rack, diff index_tag
            //xor, do not consider order
            if (buffer) //may local, or failed
            {
                printf("\tchunk_list[%u][%u], used_size=%u, KV_num=%u\n", tmp->index_tag, tmp->chunk_id, ech->chunk_list[i][tmp->chunk_id].used_size, ech->chunk_list[i][tmp->chunk_id].KV_num);
                VERBOSE(3, "\tIn this rack (%u,%u) is ok\n", i, tmp->chunk_id);
                unsigned char *pp = transfer_str_to_ustr(buffer, CHUNK_SIZE);
                memcpy(tmp->left_data[tmp->need++], pp, CHUNK_SIZE);
                free(pp);
                //free(buffer);
            }
        }
    }
    //local in
    if (tmp->chunk_id % RACK == rid_self)
    {
        size_t value_length;
        uint32_t flags;
        memcached_return_t rc;
        char key_local[100] = {0};
        sprintf(key_local, "local-%d-%d", gid_self, tmp->chunk_id);
        char *local = memcached_get(ech->ring, key_local, strlen(key_local), &value_length, &flags, &rc);
        if (local)
        {
            VERBOSE(3, "\tIn this rack, local parity{%s} is ok\n", key_local);
            unsigned char *pp = transfer_str_to_ustr(local, CHUNK_SIZE);
            show_local(pp);
            memcpy(tmp->left_data[tmp->need++], pp, CHUNK_SIZE);
            free(pp);
        }
        else
        {
            //fill in
            memset(tmp->left_data[tmp->need++], 'f', CHUNK_SIZE);
            VERBOSE(3, "\tIn this rack, local parity{%s} is nok, maybe not encoded\n", key_local);
        }
    }

    if (tmp->need == RACK - 1 + NODE - 1)
    {
        pthread_mutex_lock(&local_repair_mutex);
        local_can_repair++;
        if (local_can_repair > 0)
        {
            //wake local repair
            VERBOSE(3, "\nJust Now, waking up local_repair for kv{%s}\n", tmp->key);
            pthread_cond_signal(&local_repair_cond);
        }
        pthread_mutex_unlock(&local_repair_mutex);
    }
    else
    {
        //other rack
        for (int i = 0; i < RACK; i++)
        {
            if (i == rid_self)
                continue;

            //notify other rack to provide data
            char send_com[COMMAND_SIZE];
            memset(send_com, 0, COMMAND_SIZE);

            //l-gather-middle, (gid rid_self) chunkid
            sprintf(send_com, "%s %d %d %u", "l-gather-middle", gid_self, rid_self, tmp->chunk_id);

            int ret = send(connfd_list_W[gid_self][i], send_com, COMMAND_SIZE, 0);
            if (-1 == ret)
                print_err("send failed", errno);
            else
                VERBOSE(3, "\n\t****(Local middle SEND command) (%s) to (%d,%d)\n", send_com, gid_self, i);
        }
    }
}

void *local_repair(void *arg)
{
    pthread_mutex_init(&local_repair_mutex, NULL);
    pthread_cond_init(&local_repair_cond, NULL);

    while (1)
    {
        pthread_mutex_lock(&local_repair_mutex);

        if (local_can_repair == 0)
        {
            VERBOSE(3, "\nChoking in local_repair\n");
            pthread_cond_wait(&local_repair_cond, &local_repair_mutex);
        }

        VERBOSE(3, "\nResuming in local_repair\n");
        struct local_repair_arg *p = local_repair_list, *q = p;

        //repair
        //find can encode_st
        if (p == NULL)
        {
            VERBOSE(3, "\nReapair_list == NULL\n");
        }

        if (p->need == RACK - 1 + NODE - 1)
        {
            local_repair_list = p->next;
        }
        else
        {
            p = p->next;
            while (p)
            {
                if (p->need == RACK - 1 + NODE - 1)
                {
                    q->next = p->next;
                    break;
                }
                q = p;
                p = p->next;
            }
        }

        local_repair_wait_num--;
        local_can_repair--;

        pthread_mutex_unlock(&local_repair_mutex);

        //decode
        VERBOSE(3, "Repair is ready\n");
        unsigned char *fail_chunk = l_decode(p->left_data, p->recovery_data, p->need);

        //get value by offset and length
        char *v = transfer_ustr_to_str(fail_chunk, CHUNK_SIZE);
        char *value = (char *)calloc(1, (p->length + 1) * sizeof(char));
        memcpy(value, v + p->offset, p->length);
        free(v);

        //get failed value
        //VERBOSE(4,"\n\nGET FAILED VALUE key:{%s} ==> {%s}\n\n", p->key, value);
        show_data(value, p->length, "Target data");
        free(value);

        //send to server
        // char send_buf[CHUNK_SIZE];
        // memset(send_buf,0,CHUNK_SIZE);

        // sprintf(send_buf, "%s %s %s", "recovery", p->key, value);

        // int ret = send(connfd_server, send_buf, CHUNK_SIZE, 0);
        // if (-1 == ret)
        //     print_err("send failed", errno);
        // else
        // {
        //     VERBOSE(1, "\n\t$$$$(Recovery data SEND command) to server\n", send_buf);
        // }

        gettimeofday(&(p->end), NULL);

        double time = timeval_diff(&(p->begin), &(p->end));

        FILE *fout = fopen("repair.txt", "a+");

        VERBOSE(4, "\n\nRepair kv{%s} time: %.1f us\n\n", p->key, time);

        //fprintf(stderr,"\n\n In (gid,rid)=(%d,%d) Repair kv{%s} time: %.10f s\n\n*********************************************\n", gid_self, rid_self, p->key, time);
        fprintf(fout, "%.1f\n", time);
        fclose(fout);

        //sleep(1);

        //test
        static int tt = 1;
        if (tt < 100)
        {
            tt++;
            //put this into local_repair again
            pthread_mutex_lock(&local_repair_mutex);

            p->need = 0;
            p->next = NULL;
            local_repair_list = p;

            //time start
            gettimeofday(&(p->begin), NULL);

            for (int i = 0; i < RACK - 1 + NODE - 1; i++)
            {
                memset(local_repair_list->left_data[i], 0, CHUNK_SIZE);
            }
            //for one
            memset(local_repair_list->recovery_data, 0, CHUNK_SIZE);

            threadpool_add_job(repair_pool, repair_chunk, local_repair_list);

            pthread_mutex_unlock(&local_repair_mutex);

            // for(int i=0;i<RACK-1+NODE-1;i++)
            // {
            //     free(p->left_data[i]);
            // }

            // free(p->recovery_data);
            // free(p);
        }
        // else //kill background
        // {
        //     system("ps -ef|grep ./back |cut -c 9-15|xargs kill -9");
        // }

        //kill requestor
        // system("ps -ef|grep ./requestor |cut -c 9-15|xargs kill -9");
        // exit(-1);
    }
}

//work with server, for normal KV
void *work(void *arg)
{
    char send_buf[CHUNK_SIZE];
    char receive_buf[CHUNK_SIZE];

    int fd = (long)arg;
    enum status status_now = conn_init;

    while (status_now != conn_close)
    {
        switch (status_now)
        {
        case conn_wait:
        {
            status_now = conn_read;
            //sleep(1);
            break;
        }
        case conn_read:
        {
            //receive message
            memset(receive_buf, 0, CHUNK_SIZE);
            int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
            if (-1 == ret)
            {
                status_now = conn_close;
                print_err("recv failed", errno);
            }
            else
            {
                VERBOSE(1, "[GET request,%d]<={%s}\n", ret, receive_buf);

                if (strcmp(receive_buf, "quit") == 0)
                {
                    status_now = conn_close;
                }

                //memcached set
                char key[100];

                if (strncmp(receive_buf, "set", 3) == 0) //set
                {
                    char value[CHUNK_SIZE] = {0};

                    sscanf(receive_buf, "%*s %s %s", key, value);

                    //for(int i=0;i<20;i++)
                    //    memcpy(value+20*i,str,20);
                    //memset(value,key[4],500);

                    rc = ECHash_set(ech, key, strlen(key), value, strlen(value), 0, 0);
                    if (rc == MEMCACHED_SUCCESS)
                    {
                        VERBOSE(1, "\tSET:[%s] ok\n", key);
                        sprintf(send_buf, "ack kv{%s} STORED OK", key);
                    }
                    else
                    {
                        VERBOSE(1, "\tSET:[%s] not ok\n", key);
                        sprintf(send_buf, "ack kv{%s} STORED NOK", key);
                    }

                    //check sealed data chunk
                    char buffer[CHUNK_SIZE];
                    int index_tag;
                    uint32_t chunk_id = 0;
                    if ((index_tag = check_chunk_sealed(ech, buffer, &chunk_id)) != -1)
                    {
                        //show_data(buffer,CHUNK_SIZE);
                        //VERBOSE(1,"\n\n\n\n\nget a sealed data chunk,chunk_id=%d, index_tag=%d\n\n\n\n\n\n\n",chunk_id,index_tag);
                        //local parity
                        if (chunk_id % RACK == rid_self)
                        {
                            pthread_mutex_lock(&local_encode_mutex);

                            if (local_encode_list == NULL)
                            {
                                local_encode_list = (struct local_encode_st *)calloc(1, sizeof(struct local_encode_st));
                                local_encode_list->chunk_id = chunk_id;
                                local_encode_list->num++;
                                local_encode_list->next = NULL;
                                local_wait_num++;

                                //initial the encode
                                for (int i = 0; i < LN; i++)
                                {
                                    local_encode_list->source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                                }

                                if (local_encode_list->num == LK)
                                {
                                    local_can_encode++;
                                }
                                unsigned char *pp = transfer_str_to_ustr(buffer, CHUNK_SIZE);
                                memcpy(local_encode_list->source_data[local_encode_list->num - 1], pp, CHUNK_SIZE);
                                free(pp);
                                //show_unsigned_data(local_encode_list->source_data[local_encode_list->num-1],CHUNK_SIZE);
                                VERBOSE(2, "\n\t****Local data(%d,%d) NEW (%d), local_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", gid_self, rid_self, local_encode_list->num, local_wait_num, local_can_encode, chunk_id, index_tag);
                            }
                            else
                            {
                                //local parity in this rack, do not need to send
                                struct local_encode_st *p = local_encode_list, *q = p;
                                while (p)
                                {
                                    if (p->chunk_id == chunk_id)
                                    {
                                        p->num++;
                                        if (p->num == LK)
                                        {
                                            local_can_encode++;
                                        }
                                        unsigned char *pp = transfer_str_to_ustr(buffer, CHUNK_SIZE);
                                        memcpy(p->source_data[p->num - 1], pp, CHUNK_SIZE);
                                        free(pp);
                                        //show_unsigned_data(p->source_data[p->num-1],CHUNK_SIZE);
                                        VERBOSE(2, "\n\t****Local data(%d,%d) OLD (%d), local_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", gid_self, rid_self, p->num, local_wait_num, local_can_encode, chunk_id, index_tag);
                                        break;
                                    }
                                    q = p;
                                    p = p->next;
                                }
                                if (p == NULL)
                                {
                                    struct local_encode_st *new_encode = (struct local_encode_st *)calloc(1, sizeof(struct local_encode_st));
                                    new_encode->chunk_id = chunk_id;
                                    new_encode->num++;
                                    new_encode->next = NULL;
                                    q->next = new_encode;
                                    local_wait_num++;

                                    for (int i = 0; i < LN; i++)
                                    {
                                        new_encode->source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                                    }

                                    if (new_encode->num == LK)
                                    {
                                        local_can_encode++;
                                    }
                                    unsigned char *pp = transfer_str_to_ustr(buffer, CHUNK_SIZE);
                                    memcpy(new_encode->source_data[new_encode->num - 1], pp, CHUNK_SIZE);
                                    free(pp);
                                    //show_unsigned_data(new_encode->source_data[new_encode->num-1], CHUNK_SIZE);
                                    VERBOSE(2, "\n\t****Local data(%d,%d) new (%d), local_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", gid_self, rid_self, new_encode->num, local_wait_num, local_can_encode, chunk_id, index_tag);
                                }
                            }

                            if (local_can_encode > 0)
                            {
                                //wake up local parity
                                VERBOSE(2, "\nWaking up local_encode in (local)\n");
                                pthread_cond_signal(&local_encode_cond);
                            }

                            //show_local_encode();
                            pthread_mutex_unlock(&local_encode_mutex);
                        }
                        else
                        {
                            struct send_arg *ll = (struct send_arg *)calloc(1, sizeof(struct send_arg));
                            ll->connfd = connfd_list_W[gid_self][chunk_id % RACK];
                            ll->global = 0;
                            //come from
                            ll->gid = ech->gid;
                            ll->rid = ech->rid;
                            ll->index_tag = index_tag;
                            ll->chunk_id = chunk_id;
                            memcpy(ll->send, buffer, CHUNK_SIZE);

                            //show_data(ll->send, CHUNK_SIZE);
                            //send to this group, chunkID%RACK
                            VERBOSE(2, "\n\t****Add [Local data] task to (%d,%d) local_encode_st, chunk_id=%d,index_tag=%d\n", gid_self, chunk_id % RACK, chunk_id, index_tag);
                            threadpool_add_job(send_pool, proxy_send, ll);
                        }

                        //global parity
                        //send to chunk_id%GROUP, chunk_id%RACK
                        //all needed to send
                        struct send_arg *gg = (struct send_arg *)calloc(1, sizeof(struct send_arg));
                        gg->connfd = connfd_list_W[chunk_id % GROUP][chunk_id % RACK];
                        gg->global = 1;
                        //come from
                        gg->gid = ech->gid;
                        gg->rid = ech->rid;
                        gg->index_tag = index_tag;
                        gg->chunk_id = chunk_id;
                        memcpy(gg->send, buffer, CHUNK_SIZE);

                        //show_data(gg->send, CHUNK_SIZE);
                        //send to this group, chunkID%RACK
                        VERBOSE(2, "\n\t####Add [Global data] task to (%d,%d) global_encode_st, chunk_id=%d,index_tag=%d\n", chunk_id % GROUP, chunk_id % RACK, chunk_id, index_tag);
                        threadpool_add_job(send_pool, proxy_send, gg);
                    }

                    status_now = conn_write;
                    break;
                }
                else if (strncmp(receive_buf, "get", 3) == 0) //get
                {
                    sscanf(receive_buf, "%*s %s", key);
                    size_t val_len;
                    uint32_t flags;

                    // include dget
                    char *getval = ECHash_get(ech, key, strlen(key), &val_len, &flags, &rc);

                    //failed test
                    //if(getval[0] > 'T')
                    rc = MEMCACHED_FAILURE;

                    if (rc == MEMCACHED_SUCCESS)
                    {
                        VERBOSE(1, "\tGET:[%s] ok\n", key);
                        sprintf(send_buf, "value %s %s (%d,%d)", key, getval, gid_self, rid_self);
                    }
                    else
                    {
                        //degraded read data
                        //local_repair_list
                        uint64_t value = get_value_hash_table(ech->hash_table, key);

                        uint32_t index_tag = (uint32_t)(value >> 56);
                        uint32_t chunk_id = (uint32_t)(value >> 24) & 0xffffffff;
                        uint32_t offset = (uint32_t)(value >> 12) & 0xfff;
                        uint32_t length = (uint32_t)(value & 0xfff);

                        if (ech->chunk_list[index_tag][chunk_id].stat == Sealed) //encoded, then put into repair_list
                        {
                            pthread_mutex_lock(&local_repair_mutex);
                            if (local_repair_list == NULL)
                            {
                                local_repair_list = (struct local_repair_arg *)calloc(1, sizeof(struct local_repair_arg));
                                strcpy(local_repair_list->key, key);
                                local_repair_list->need = 0;
                                local_repair_list->next = NULL;
                                local_repair_list->index_tag = index_tag;
                                local_repair_list->chunk_id = chunk_id; //mark the repair unit
                                local_repair_list->offset = offset;
                                local_repair_list->length = length;

                                //time start
                                gettimeofday(&(local_repair_list->begin), NULL);

                                for (int i = 0; i < RACK - 1 + NODE - 1; i++)
                                {
                                    local_repair_list->left_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                                }
                                //for one
                                local_repair_list->recovery_data = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));

                                threadpool_add_job(repair_pool, repair_chunk, local_repair_list);
                            }
                            else
                            {
                                //tail insert
                                struct local_repair_arg *p = local_repair_list, *q = p;
                                while (p)
                                {
                                    q = p;
                                    p = p->next;
                                }

                                struct local_repair_arg *new_repair = (struct local_repair_arg *)calloc(1, sizeof(struct local_repair_arg));
                                strcpy(new_repair->key, key);
                                new_repair->need = 0;
                                new_repair->next = NULL;
                                new_repair->index_tag = index_tag;
                                new_repair->chunk_id = chunk_id; //mark the repair unit
                                new_repair->offset = offset;
                                new_repair->length = length;

                                gettimeofday(&(new_repair->begin), NULL);

                                for (int i = 0; i < RACK - 1 + NODE - 1; i++)
                                {
                                    new_repair->left_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                                }
                                //for one
                                new_repair->recovery_data = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));

                                q->next = new_repair;
                                threadpool_add_job(repair_pool, repair_chunk, new_repair);
                            }

                            pthread_mutex_unlock(&local_repair_mutex);
                        }
                        else
                        {
                            VERBOSE(3, "\n\t$$$$CANOT DECODE, index_tag=%u, chunk_id=%u, offset=%u, length=%u\n", index_tag, chunk_id, offset, length);
                        }

                        strcpy(send_buf, "value nok, START DEGRARED READ!");
                    }

                    status_now = conn_write;
                    break;
                }
                else if (strncmp(receive_buf, "update", 6) == 0) //update
                {
                    char value[CHUNK_SIZE] = {0};

                    sscanf(receive_buf, "%*s %s %s", key, value);

                    sleep(1);

                    uint64_t vv = get_value_hash_table(ech->hash_table, key);
                    uint32_t index_tag = (uint32_t)(vv >> 56);
                    uint32_t chunk_id = (uint32_t)(vv >> 24) & 0xffffffff;
                    uint32_t offset = (uint32_t)(vv >> 12) & 0xfff;
                    uint32_t length = (uint32_t)(vv & 0xfff);

                    //get orginal
                    size_t val_len;
                    uint32_t flags;

                    char *getval = memcached_get(ech->ring, key, strlen(key), &val_len, &flags, &rc);
                    rc = memcached_set(ech->ring, key, strlen(key), value, strlen(value), 0, 0);

                    char delta[CHUNK_SIZE] = {0};
                    memset(delta, 0, CHUNK_SIZE);
                    //xor, some difference
                    int i, j;
                    for (i = offset, j = 0; i < length; i++)
                    {
                        delta[i] = delta[i] ^ getval[j++];
                    }

                    //failed
                    if (rc != MEMCACHED_SUCCESS)
                    {
                        VERBOSE(4, "\tUPDATE:[%s] nok\n", key);
                        sprintf(send_buf, "ack kv{%s} UPDATE NOK", key);
                    }
                    else
                    {
                        if (ech->chunk_list[index_tag][chunk_id].stat == Sealed) //encoded, then put into repair_list
                        {
                            //send to local, same rack
                            if (chunk_id % RACK == rid_self)
                            {
                                gettimeofday(&l_this_update_begin, NULL);
                                //get local parity
                                char key_local[100] = {0};
                                sprintf(key_local, "local-%d-%d", gid_self, chunk_id);
                                char *local = memcached_get(ech->ring, key_local, strlen(key_local), &val_len, &flags, &rc);

                                //xor
                                for (int i = 0; i < CHUNK_SIZE; i++)
                                {
                                    delta[i] = delta[i] ^ local[i];
                                }

                                //update the local
                                rc = memcached_set(ech->ring, key_local, strlen(key_local), local, strlen(local), 0, 0);
                                if (rc == MEMCACHED_SUCCESS)
                                {
                                    VERBOSE(4, "update local in this rack ok\n");
                                }
                                else
                                {
                                    VERBOSE(4, "update local in this rack nok\n");
                                }
                                gettimeofday(&l_this_update_end, NULL);

                                double time = timeval_diff(&l_this_update_begin, &l_this_update_end);
                                FILE *fin = fopen("l_this_rack_update.txt", "a+");

                                VERBOSE(4, "\nUpdate this rack local kv{} time: %.1f us\n\n", time);

                                //fprintf(fin,"\n\n In (gid,rid)=(%d,%d) Repair kv{%s} time: %.10f s\n\n*********************************************\n", gid_self, rid_self, p->key, time);
                                fprintf(fin, "%.1f\n", time);
                                fclose(fin);
                            }
                            else //send to other local parity, other rack
                            {
                                //update++
                                rack_update[gid_self][rid_self]++;

                                struct update_arg *uu = (struct update_arg *)calloc(1, sizeof(struct update_arg));
                                uu->connfd = connfd_list_W[gid_self][chunk_id % RACK];
                                uu->gid = gid_self;
                                uu->rid = rid_self;
                                uu->global = 0;
                                uu->chunk_id = chunk_id;
                                memcpy(uu->update, delta, CHUNK_SIZE);

                                gettimeofday(&l_other_update_begin, NULL);

                                threadpool_add_job(update_pool, update_send, uu);
                                VERBOSE(4, "update in other racks\n");
                            }

                            //send to global parity
                            struct update_arg *uu = (struct update_arg *)calloc(1, sizeof(struct update_arg));
                            uu->connfd = connfd_list_W[chunk_id % GROUP][chunk_id % RACK];
                            uu->gid = gid_self;
                            uu->rid = rid_self;
                            uu->global = 1;
                            uu->chunk_id = chunk_id;
                            memcpy(uu->update, delta, CHUNK_SIZE);
                            gettimeofday(&g_update_begin, NULL);

                            threadpool_add_job(update_pool, update_send, uu);
                            VERBOSE(4, "update in global racks\n");
                        }
                        else
                        {
                            VERBOSE(4, "\nNOT encode\n", key);
                            strcat(send_buf, ", but not encoded");
                        }
                    }
                    status_now = conn_write;
                    break;
                }
            }
            break;
        }
        case conn_write:
        {
            // scanf("%[^\n]",send_buf);
            // getchar();
            //VERBOSE("s=%s\n",send_buf);

            //sprintf(send_buf,"I receive [%s]!",receive_buf);
            //sleep(1);
            int ret = send(fd, send_buf, CHUNK_SIZE, 0);
            if (-1 == ret)
                print_err("send failed", errno);
            else
                VERBOSE(1, "\t[SEND request ack,%d]=>{%s}\n", ret, send_buf);

            status_now = conn_read;
            memset(send_buf, 0, CHUNK_SIZE);
            break;
        }
        case conn_init:
        {
            //getchar();
            memset(send_buf, 0, CHUNK_SIZE);
            sprintf(send_buf, "CONN %d %d", gid_self, rid_self);
            // scanf("%[^\n]",send_buf);
            // getchar();
            //VERBOSE(1,"s=%s\n",send_buf);

            //sprintf(send_buf,"I receive [%s]!",receive_buf);
            //sleep(1);
            int ret = send(fd, send_buf, COMMAND_SIZE, 0);
            if (-1 == ret)
                print_err("send failed", errno);
            else
                VERBOSE(1, "\t[SEND connect,%d]=>{%s}\n", ret, send_buf);

            status_now = conn_wait;
            memset(send_buf, 0, CHUNK_SIZE);
            break;
        }
        case conn_close:
        {
            VERBOSE(1, "\n\nconn_close\n");
            break;
        }
        }
    }
    close(fd);
}

//receive data chunks
//we accpet, the former
void *accept_proxy_rece(void *arg)
{
    pthread_mutex_t rece_mutex = PTHREAD_MUTEX_INITIALIZER;

    //char send_buf[CHUNK_SIZE];
    char receive_com[COMMAND_SIZE];
    char receive_buf[CHUNK_SIZE];

    int gid, rid;
    int fd = (long)arg;
    enum status status_now = conn_init;

    while (status_now != conn_close)
    {
        switch (status_now)
        {
        case conn_wait:
        {
            status_now = conn_read;
            //sleep(1);
            break;
        }
        case conn_init:
        {
            //receive message
            memset(receive_com, 0, COMMAND_SIZE);

            pthread_mutex_lock(&rece_mutex);

            int ret = recv(fd, receive_com, COMMAND_SIZE, 0);
            pthread_mutex_unlock(&rece_mutex);
            if (-1 == ret)
            {
                status_now = conn_close;
                print_err("recv failed", errno);
            }
            else
            {
                //parse_handle(receive_buf, ret);
                //set connection fd
                VERBOSE(1, "\t[GET connect,%d]=>{%s}\n", ret, receive_com);
                if (strncmp(receive_com, "CONN", 4) == 0)
                {
                    sscanf(receive_com, "%*s %d %d", &gid, &rid);
                    VERBOSE(1, "\t[MANAGE CONN gid=%d, rid=%d]\n", gid, rid);

                    connfd_list_R[gid][rid] = fd;
                    VERBOSE(1, "Accept connfd_list_R: ");
                    for (int i = 0; i < GROUP; i++)
                    {
                        for (int j = 0; j < RACK; j++)
                            VERBOSE(1, "%d, ", connfd_list_R[i][j]);
                    }
                    VERBOSE(1, "\n");
                }
            }

            status_now = conn_wait;
            break;
        }
        case conn_read:
        {
            //receive large data chunks, but it will receive the ack, ignore them
            memset(receive_com, 0, COMMAND_SIZE);
            pthread_mutex_lock(&rece_mutex);
            int ret = recv(fd, receive_com, COMMAND_SIZE, 0);
            pthread_mutex_unlock(&rece_mutex);
            if (-1 == ret)
            {
                status_now = conn_close;
                print_err("recv failed", errno);
                //strcpy(send_buf,"ack NOK");
                //status_now=conn_write;
                status_now = conn_wait;
                break;
            }
            // else if(strncmp(receive_buf,"ack",3)==0) //may receive the other ack message
            // {
            //     VERBOSE(1,"\t[GET data chunk ack,%d]=>{%s}\n",ret,receive_buf);
            //     status_now=conn_read;
            //     break;
            // }
            else if (strncmp(receive_com, "l-encode", 8) == 0) //local data chunk
            {
                VERBOSE(2, "\t(Local data RECE command) {%s}\n", receive_com);

                memset(receive_buf, 0, CHUNK_SIZE);
                pthread_mutex_lock(&rece_mutex);
                int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
                pthread_mutex_unlock(&rece_mutex);

                //show_data(receive_buf,CHUNK_SIZE,"l-encode");

                //put local parity
                int g, r;
                uint32_t index_tag;
                uint32_t chunk_id;
                sscanf(receive_com, "%*s %d %d %u %u", &g, &r, &index_tag, &chunk_id);

                //set connection fd
                //VERBOSE(2,"\t[GET data chunk, %d]=>{%s}\n",ret,receive_buf);
                VERBOSE(2, "\t(Local data RECE real) from (%d,%d) chunk_id=%d)=>{data chunk}\n", g, r, chunk_id);

                pthread_mutex_lock(&local_encode_mutex);

                if (local_encode_list == NULL)
                {
                    local_encode_list = (struct local_encode_st *)calloc(1, sizeof(struct local_encode_st));
                    local_encode_list->chunk_id = chunk_id;
                    local_encode_list->num++;
                    local_encode_list->next = NULL;
                    local_wait_num++;

                    //initial the encode
                    for (int i = 0; i < LN; i++)
                    {
                        local_encode_list->source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                    }

                    if (local_encode_list->num == LK)
                    {
                        local_can_encode++;
                    }
                    unsigned char *pp = transfer_str_to_ustr(receive_buf, CHUNK_SIZE);
                    memcpy(local_encode_list->source_data[local_encode_list->num - 1], pp, CHUNK_SIZE);
                    free(pp);
                    //show_unsigned_data(local_encode_list->source_data[local_encode_list->num-1],CHUNK_SIZE);
                    VERBOSE(2, "\n\t****Remote local data from (%d,%d) NEW (%d), local_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", g, r, local_encode_list->num, local_wait_num, local_can_encode, chunk_id, index_tag);
                }
                else
                {
                    struct local_encode_st *p = local_encode_list, *q = p;
                    while (p)
                    {
                        if (p->chunk_id == chunk_id)
                        {
                            p->num++;
                            if (p->num == LK)
                            {
                                local_can_encode++;
                            }
                            unsigned char *pp = transfer_str_to_ustr(receive_buf, CHUNK_SIZE);
                            memcpy(p->source_data[p->num - 1], pp, CHUNK_SIZE);
                            free(pp);
                            //show_unsigned_data(p->source_data[p->num-1],CHUNK_SIZE);
                            VERBOSE(2, "\n\t****Remote local data from (%d,%d) OLD (%d), local_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", g, r, p->num, local_wait_num, local_can_encode, chunk_id, index_tag);
                            break;
                        }
                        q = p;
                        p = p->next;
                    }
                    if (p == NULL)
                    {
                        struct local_encode_st *new_encode = (struct local_encode_st *)calloc(1, sizeof(struct local_encode_st));
                        new_encode->chunk_id = chunk_id;
                        new_encode->num++;
                        new_encode->next = 0;
                        q->next = new_encode;
                        local_wait_num++;

                        for (int i = 0; i < LN; i++)
                        {
                            new_encode->source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                        }

                        if (new_encode->num == LK)
                        {
                            local_can_encode++;
                        }
                        unsigned char *pp = transfer_str_to_ustr(receive_buf, CHUNK_SIZE);
                        memcpy(new_encode->source_data[new_encode->num - 1], pp, CHUNK_SIZE);
                        free(pp);
                        //show_unsigned_data(new_encode->source_data[new_encode->num-1],CHUNK_SIZE);
                        VERBOSE(2, "\n\t****Remote local data from (%d,%d) new (%d), local_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", g, r, new_encode->num, local_wait_num, local_can_encode, chunk_id, index_tag);
                    }
                }

                if (local_can_encode > 0)
                {
                    //wake local parity
                    VERBOSE(2, "\nWaking up local_encode in (receiving)\n");
                    pthread_cond_signal(&local_encode_cond);
                }

                //show_local_encode();
                pthread_mutex_unlock(&local_encode_mutex);

                //strcpy(send_buf,"ack local OK");
                //status_now=conn_write;
                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "g-encode", 8) == 0) //global data chunk
            {
                VERBOSE(2, "\t(Global data RECE command) {%s}\n", receive_com);

                memset(receive_buf, 0, CHUNK_SIZE);
                pthread_mutex_lock(&rece_mutex);
                int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
                pthread_mutex_unlock(&rece_mutex);

                //show_data(receive_buf,CHUNK_SIZE);

                //put global parity
                int g, r;
                uint32_t index_tag;
                uint32_t chunk_id;
                sscanf(receive_com, "%*s %d %d %u %u", &g, &r, &index_tag, &chunk_id);

                //set connection fd
                //VERBOSE(2,"\t[GET data chunk, %d]=>{%s}\n",ret,receive_buf);
                VERBOSE(2, "\t(Global data RECE real) from (%d,%d) chunk_id=%d]=>{data chunk}\n", g, r, chunk_id);

                pthread_mutex_lock(&global_encode_mutex);

                //printf("g,r,index_tag, chunkid =(%d,%d,%u,%u), GN=%d, GK=%d, offset=%u\n",g,r,index_tag,chunk_id, GN, GK, offset(g, r, index_tag, gid_self));
                if (global_encode_list == NULL)
                {
                    global_encode_list = (struct global_encode_st *)calloc(1, sizeof(struct global_encode_st));
                    global_encode_list->chunk_id = chunk_id;
                    global_encode_list->num++;
                    global_encode_list->next = 0;
                    global_wait_num++;

                    //initial the encode
                    for (int i = 0; i < GN; i++)
                    {

                        global_encode_list->source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                    }

                    if (global_encode_list->num == GK)
                    {
                        global_can_encode++;
                    }
                    unsigned char *pp = transfer_str_to_ustr(receive_buf, CHUNK_SIZE);
                    //printf("GN=%d, GK=%d, offset=%u\n",GN, GK, offset(g, r, index_tag, gid_self));
                    memcpy(global_encode_list->source_data[global_encode_list->num - 1], pp, CHUNK_SIZE);
                    //memcpy(global_encode_list->source_data[offset(g,r,index_tag,gid_self)], pp, CHUNK_SIZE);
                    free(pp);
                    //show_unsigned_data(global_encode_list->source_data[global_encode_list->num-1], CHUNK_SIZE);
                    VERBOSE(2, "\n\t####Remote global data from (%d,%d) NEW (%d), global_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", g, r, global_encode_list->num, global_wait_num, global_can_encode, chunk_id, index_tag);
                }
                else
                {
                    struct global_encode_st *p = global_encode_list, *q = p;
                    while (p)
                    {
                        if (p->chunk_id == chunk_id)
                        {
                            p->num++;
                            if (p->num == GK)
                            {
                                global_can_encode++;
                            }
                            unsigned char *pp = transfer_str_to_ustr(receive_buf, CHUNK_SIZE);
                            //memcpy(p->source_data[offset(g,r,index_tag,gid_self)], pp, CHUNK_SIZE);
                            memcpy(p->source_data[p->num - 1], pp, CHUNK_SIZE);
                            free(pp);
                            // //how_unsigned_data(p->source_data[p->num-1], CHUNK_SIZE);
                            VERBOSE(2, "\n\t####Remote global data from (%d,%d) OLD (%d), global_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", g, r, p->num, global_wait_num, global_can_encode, chunk_id, index_tag);
                            break;
                        }
                        q = p;
                        p = p->next;
                    }
                    if (p == NULL)
                    {
                        struct global_encode_st *new_encode = (struct global_encode_st *)calloc(1, sizeof(struct global_encode_st));
                        new_encode->chunk_id = chunk_id;
                        new_encode->num++;
                        new_encode->next = 0;
                        q->next = new_encode;
                        global_wait_num++;

                        for (int i = 0; i < GN; i++)
                        {
                            new_encode->source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
                        }

                        if (new_encode->num == GK)
                        {
                            global_can_encode++;
                        }
                        unsigned char *pp = transfer_str_to_ustr(receive_buf, CHUNK_SIZE);
                        //memcpy(new_encode->source_data[offset(g,r,index_tag,gid_self)], pp, CHUNK_SIZE);
                        memcpy(new_encode->source_data[new_encode->num - 1], pp, CHUNK_SIZE);
                        free(pp);
                        // show_unsigned_data(new_encode->source_data[new_encode->num-1], CHUNK_SIZE);
                        VERBOSE(2, "\n\t####Remote global data from (%d,%d) NEW (%d), global_wait_num=%d, can_encode=%d, chunk_id=%d, index_tag=%d\n", g, r, new_encode->num, global_wait_num, global_can_encode, chunk_id, index_tag);
                    }
                }

                if (global_can_encode > 0)
                {
                    //wake local parity
                    VERBOSE(2, "\nWaking up global_encode in (receiving)\n");
                    pthread_cond_signal(&global_encode_cond);
                }
                pthread_mutex_unlock(&global_encode_mutex);

                //strcpy(send_buf,"ack global OK");
                //status_now=conn_write;
                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "l-gather-middle", 15) == 0) //receive gather middle
            {
                VERBOSE(3, "\t(Local-gather middle RECE command) {%s}\n", receive_com);

                uint32_t gid, rid;
                uint32_t chunk_id;
                sscanf(receive_com, "%*s %d %d %u", &gid, &rid, &chunk_id);

                struct gather_arg *ga = (struct gather_arg *)calloc(1, sizeof(struct gather_arg));
                ga->connfd = connfd_list_W[gid][rid];
                ga->chunk_id = chunk_id;
                ga->rid = rid;

                threadpool_add_job(gather_pool, gather_middle, ga);
                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "g-gather-middle", 15) == 0) //receive gather middle
            {
            }
            else if (strncmp(receive_com, "l-middle", 8) == 0) //receve local middle
            {
                VERBOSE(3, "\t(Local middle RECE command) {%s}\n", receive_com);

                memset(receive_buf, 0, CHUNK_SIZE);
                pthread_mutex_lock(&rece_mutex);
                int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
                pthread_mutex_unlock(&rece_mutex);

                //show_data(receive_buf,CHUNK_SIZE);
                //put local middle
                int g, r;
                uint32_t chunk_id;
                sscanf(receive_com, "%*s %d %d %u", &g, &r, &chunk_id);

                //set connection fd
                VERBOSE(3, "\t(Local middle RECE real) from (%d,%d) chunk_id=%d)=>{middle}\n", g, r, chunk_id);

                pthread_mutex_lock(&local_repair_mutex);
                struct local_repair_arg *p = local_repair_list;
                if (local_repair_list == NULL)
                {
                    VERBOSE(3, "\nReapair_list == NULL\n");
                }
                else
                {
                    while (p)
                    {
                        if (p->chunk_id == chunk_id)
                        {
                            memcpy(p->left_data[p->need++], receive_buf, CHUNK_SIZE);
                            break;
                        }
                        p = p->next;
                    }

                    if (p->need == RACK - 1 + NODE - 1)
                    {
                        local_can_repair++;
                    }
                }

                if (local_can_repair > 0)
                {
                    //wake local repair
                    VERBOSE(3, "\nWaking up local_repair for kv{%s}\n", p->key);
                    pthread_cond_signal(&local_repair_cond);
                }
                pthread_mutex_unlock(&local_repair_mutex);

                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "g-middle", 8) == 0) //receve global middle
            {
            }
            else if (strncmp(receive_com, "l-update", 8) == 0) //receive local update
            {
                VERBOSE(4, "\t(Local update RECE command) {%s}\n", receive_com);

                memset(receive_buf, 0, CHUNK_SIZE);
                pthread_mutex_lock(&rece_mutex);
                int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
                pthread_mutex_unlock(&rece_mutex);
                show_unsigned_data((unsigned char *)receive_buf, CHUNK_SIZE, "Local update receive");

                //put local update
                int g, r;
                uint32_t chunk_id;
                sscanf(receive_com, "%*s %d %d %u", &g, &r, &chunk_id);

                //set connection fd
                //VERBOSE(4,"\t[GET data chunk, %d]=>{%s}\n",ret,receive_buf);
                VERBOSE(4, "\t(Local update RECE real) from (%d,%d) chunk_id=%d)=>{update}\n", g, r, chunk_id);

                size_t val_len;
                uint32_t flags;
                //update local parity
                char key_local[100] = {0};
                sprintf(key_local, "local-%d-%d", gid_self, chunk_id);
                char *local = memcached_get(ech->ring, key_local, strlen(key_local), &val_len, &flags, &rc);

                //show_data(receive_buf,CHUNK_SIZE,"Update delta");
                //show_data(local,CHUNK_SIZE,"Local original parity");

                //xor
                for (int i = 0; i < CHUNK_SIZE; i++)
                {
                    receive_buf[i] = receive_buf[i] ^ local[i];
                }

                //show_data(receive_buf,CHUNK_SIZE,"Local new parity");
                //update the local
                rc = memcached_set(ech->ring, key_local, strlen(key_local), receive_buf, CHUNK_SIZE, 0, 0);
                if (rc == MEMCACHED_SUCCESS)
                {
                    VERBOSE(4, "update local rece from other rack ok\n");
                }
                else
                {
                    VERBOSE(4, "update local rece from other rack nok\n");
                }

                struct update_ack_arg *ua = (struct update_ack_arg *)calloc(1, sizeof(struct update_ack_arg));
                ua->connfd = connfd_list_W[g][r];
                ua->gid = gid_self;
                ua->rid = rid_self;
                ua->global = 0;

                threadpool_add_job(update_ack_pool, update_ack_send, ua);

                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "g-update", 8) == 0) //receive global update
            {
                VERBOSE(4, "\t(Global update RECE command) {%s}\n", receive_com);

                memset(receive_buf, 0, CHUNK_SIZE);
                pthread_mutex_lock(&rece_mutex);
                int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
                pthread_mutex_unlock(&rece_mutex);
                show_unsigned_data((unsigned char *)receive_buf, CHUNK_SIZE, "Global update receive");

                //put local update
                int g, r;
                uint32_t chunk_id;
                sscanf(receive_com, "%*s %d %d %u", &g, &r, &chunk_id);

                //set connection fd
                VERBOSE(4, "\t(Global update RECE real) from (%d,%d) chunk_id=%d)=>{update}\n", g, r, chunk_id);

                size_t val_len;
                uint32_t flags;
                //update global parity
                char key_global[GN - GK][100];
                for (int i = 0; i < GN - GK; i++)
                    memset(key_global[i], 0, 100);
                char *global[GN - GK];
                char global_new[GN - GK][CHUNK_SIZE];
                //for global
                for (int i = 0; i < GN - GK; i++)
                {
                    sprintf(key_global[i], "global-%d[%d]", chunk_id, i);
                    global[i] = memcached_get(ech->ring, key_global[i], strlen(key_global[i]), &val_len, &flags, &rc);
                    if (global[i])
                    {
                        //show_unsigned_data((unsigned char*)global[i],CHUNK_SIZE,"Global original parity");
                        memcpy(global_new[i], global[i], CHUNK_SIZE);
                    }
                }

                //how to update global,to do
                for (int i = 0; i < GN - GK; i++)
                {
                    for (int j = 0; j < CHUNK_SIZE; j++)
                    {
                        global_new[i][j] = receive_buf[j] ^ global[i][j];
                    }
                    //show_unsigned_data((unsigned char*)global_new[i],CHUNK_SIZE,"Global new parity");
                    rc = memcached_set(ech->ring, key_global[i], strlen(key_global[i]), global_new[i], CHUNK_SIZE, 0, 0);
                    if (rc == MEMCACHED_SUCCESS)
                    {
                        VERBOSE(4, "[%d]update global rece from other rack ok\n", i);
                    }
                    else
                    {
                        VERBOSE(4, "[%d]update global rece from other rack nok\n", i);
                    }
                }

                struct update_ack_arg *ua = (struct update_ack_arg *)calloc(1, sizeof(struct update_ack_arg));
                ua->connfd = connfd_list_W[g][r];
                ua->gid = gid_self;
                ua->rid = rid_self;
                ua->global = 1;

                threadpool_add_job(update_ack_pool, update_ack_send, ua);

                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "ack-l-update", 12) == 0) //receive local update
            {
                VERBOSE(4, "\t(Local update ack RECE command) {%s}\n", receive_com);

                gettimeofday(&l_other_update_end, NULL);

                double time = timeval_diff(&l_other_update_begin, &l_other_update_end);
                FILE *fin = fopen("l_other_rack_update.txt", "a+");

                VERBOSE(4, "\nUpdate other rack local kv{} time: %.1f us\n\n", time);

                //fprintf(fin,"\n\n In (gid,rid)=(%d,%d) Repair kv{%s} time: %.10f s\n\n*********************************************\n", gid_self, rid_self, p->key, time);
                fprintf(fin, "%.1f\n", time);
                fclose(fin);

                status_now = conn_wait;
                break;
            }
            else if (strncmp(receive_com, "ack-g-update", 12) == 0) //receive local update
            {
                VERBOSE(4, "\t(Global update ack RECE command) {%s}\n", receive_com);

                gettimeofday(&g_update_end, NULL);

                double time = timeval_diff(&g_update_begin, &g_update_end);
                FILE *fin = fopen("g_update.txt", "a+");

                VERBOSE(4, "\nUpdate global kv{} time: %.1f us\n\n", time);

                //fprintf(fin,"\n\n In (gid,rid)=(%d,%d) Repair kv{%s} time: %.10f s\n\n*********************************************\n", gid_self, rid_self, p->key, time);
                fprintf(fin, "%.1f\n", time);
                fclose(fin);

                status_now = conn_wait;
                break;
            }
        }
        case conn_write:
        {
            // ret = send(connfd_list_W[gid][rid],send_buf,sizeof(send_buf),0);
            // if(-1 == ret)
            //     print_err("send failed", errno);
            // else
            //     VERBOSE(1,"\t[SEND data chunk ack,%d]=>{%s}\n",ret,send_buf);

            status_now = conn_wait;
            //memset(send_buf,0,BUFFER_SIZE);
            break;
        }
        case conn_close:
        {
            VERBOSE(1, "\n\nconn_close\n");
            break;
        }
        }
    }

    close(fd);
}

//As server to connect these gid and rid higher
void *server(void *arg)
{
    int listenfd = -1, ret = -1;
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse));
    if (-1 == listenfd)
    {
        print_err("Socket failed", errno);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(P_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    printf("inet_addr=%s\n", ips[gid_self][rid_self]);
    ret = bind(listenfd, (struct sockaddr *)&addr, sizeof(addr));
    if (-1 == ret)
    {
        print_err("Bind failed", errno);
    }

    //wait for connect
    ret = listen(listenfd, LISTEN_NUM);
    if (-1 == ret)
    {
        print_err("Listen failed", errno);
    }

    int connfd = -1;
    while (1)
    {
        struct sockaddr_in caddr = {0};
        unsigned int csize = sizeof(caddr);
        //create auto scokedfd
        connfd = accept(listenfd, (struct sockaddr *)&caddr, &csize);
        if (-1 == connfd)
        {
            print_err("Accept failed", errno);
        }

        VERBOSE(1, "Cport = %d, caddr = %s\n", ntohs(caddr.sin_port), inet_ntoa(caddr.sin_addr));

        //for each proxy, create a thread to handle its communication
        pthread_t id;
        //ret = pthread_create(&id, NULL, accept_proxy_send, (void *)connfd);
        ret = pthread_create(&id, NULL, accept_proxy_rece, (void *)connfd);
        if (-1 == ret)
            print_err("Pthread create failed", errno);
    }
}

int main(int argc, char *argv[])
{
    // if (argc != 3)
    // {
    //     VERBOSE(1, "./p gid rid\n");
    //     exit(-1);
    // }
    // else
    // {
    //     sscanf(argv[1], "%d", &gid_self);
    //     sscanf(argv[2], "%d", &rid_self);
    // }

    // //create PORT
    // for(int i=0; i<GROUP;i++)
    // {
    //     for(int j=0;j<RACK;j++)
    //         P_PORT[i][j]=12001+i*RACK+j;
    // }

    //getname
    char hostname[20] = {0};
    FILE *host = fopen("/etc/hostname", "r");
    fscanf(host, "%s", hostname);
    fclose(host);

    FILE *fin = fopen("ip.ini", "r");
    char line[1024] = {0};
    int ii = 0;
    while (ii < GROUP * RACK && (!feof(fin)) && fgets(line, 1024, fin))
    {
        char tmp[100] = {0}, name[100] = {0};
        sscanf(line, "%s %s", tmp, name);
        printf("%s %s\n", tmp, name);
        if (strcmp(name, hostname) == 0)
        {
            gid_self = ii / RACK;
            rid_self = ii % RACK;
        }
        strcpy(ips[ii / RACK][ii % RACK], tmp);
        printf("ip:%s, g=%d,r=%d\n", ips[ii / RACK][ii % RACK], ii / RACK, ii % RACK);
        ii++;
    }

    printf("\n\nhostname=%s, gid_self=%d, rid_self=%d\n\n", hostname, gid_self, rid_self);
    fclose(fin);

    //memcached
    ECHash_init(&ech, GROUP, RACK, NODE, gid_self, rid_self);

    for (int i = 0; i < NODE; i++)
    {
        ECHash_init_addserver(ech, "127.0.0.1", 21000 + i);
    }

    //thread pool
    send_pool = threadpool_init(1, 10);
    repair_pool = threadpool_init(1, 10);
    gather_pool = threadpool_init(1, 10);
    middle_pool = threadpool_init(1, 10);
    update_pool = threadpool_init(1, 10);
    update_ack_pool = threadpool_init(1, 10);

    //local_encode, global_encode
    pthread_t leid, geid, lrid;
    int ret = pthread_create(&leid, NULL, local_encode, (void *)NULL);
    ret = pthread_create(&geid, NULL, global_encode, (void *)NULL);
    ret = pthread_create(&lrid, NULL, local_repair, (void *)NULL);

    //******connect server
    connfd_server = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(connfd_server, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse));
    if (-1 == connfd_server)
    {
        print_err("server socket failed", errno);
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(S_IP);
    addr.sin_port = htons(S_PORT);

    while (1)
    {
        int re = connect(connfd_server, (struct sockaddr *)&addr, sizeof(addr));
        if (-1 == re)
        {
            print_err("server connect requestor failed", errno);
            sleep(1);
        }
        else
            break;
    }

    pthread_t wid;
    ret = pthread_create(&wid, NULL, work, (void *)connfd_server);

    pthread_t sid;
    ret = pthread_create(&sid, NULL, server, (void *)NULL);
    if (-1 == ret)
        print_err("as server accept failed", errno);

    sleep(1);

    for (int i = 0; i < GROUP; i++)
    {
        for (int j = 0; j < RACK; j++)
        {
            if (i == gid_self && j == rid_self)
                continue;
            //connect proxy
            int fd_proxy = socket(AF_INET, SOCK_STREAM, 0);
            int reuse = 1;
            setsockopt(fd_proxy, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse));
            const char chOpt = 1;
            setsockopt(fd_proxy, IPPROTO_TCP, TCP_NODELAY, &chOpt, sizeof(char));

            if (-1 == fd_proxy)
            {
                print_err("proxy socket failed", errno);
            }
            struct sockaddr_in addr1;
            memset(&addr1, 0, sizeof(addr1));
            addr1.sin_family = AF_INET;
            addr1.sin_port = htons(P_PORT);

            //	if(i==0 && j==0)
            //		addr.sin_addr.s_addr = inet_addr(ip_demo);
            //	else
            addr1.sin_addr.s_addr = inet_addr(ips[i][j]);

            while (1)
            {
                int re = connect(fd_proxy, (struct sockaddr *)&addr1, sizeof(addr1));
                if (-1 == re)
                {
                    print_err("server connect failed", errno);
                    printf("number:%d %d, %s\n", i, j, ips[i][j]);
                    sleep(1);
                }
                else
                    break;
            }

            pthread_t pid;
            //send CONN and do not create a new thread
            //ret = pthread_create(&pid, NULL, conn_proxy_send, (void *)fd_proxy);
            char send_buf[COMMAND_SIZE] = {0};
            sprintf(send_buf, "CONN %d %d", gid_self, rid_self);
            int ret = send(fd_proxy, send_buf, COMMAND_SIZE, 0);
            if (-1 == ret)
                print_err("send failed", errno);
            else
                VERBOSE(1, "\t[SEND connect,%d]=>{%s}\n", ret, send_buf);

            //ret = pthread_create(&pid, NULL, conn_proxy_rece, (void *)fd_proxy);
            // if(-1 == ret)
            //     print_err("proxy connect failed", errno);

            //for sending
            connfd_list_W[i][j] = fd_proxy;
            VERBOSE(1, "Conn other groups connfd_list_W: ");
            for (int i = 0; i < GROUP; i++)
            {
                for (int j = 0; j < RACK; j++)
                    VERBOSE(1, "%d, ", connfd_list_W[i][j]);
            }
            VERBOSE(1, "\n");
        }
    }

    pthread_join(sid, NULL);
    pthread_join(wid, NULL);
    pthread_join(leid, NULL);
    pthread_join(geid, NULL);
    pthread_join(lrid, NULL);
    ECHash_destroy(ech);

    threadpool_destroy(send_pool);
    threadpool_destroy(repair_pool);
    threadpool_destroy(gather_pool);
    threadpool_destroy(middle_pool);
    threadpool_destroy(update_pool);
    threadpool_destroy(update_ack_pool);

    return 0;
}
