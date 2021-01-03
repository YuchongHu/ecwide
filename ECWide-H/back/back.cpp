#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include "libmemcached/memcached.h"

//gcc -std=c++11 back.cpp -o back -lpthread -lmemcached

#define NUM 16
memcached_return rc;
char **keys, **values;

int rack = 0;
int thread_num = 0;

int num_keys = 100;
int key_size = 20;
int value_size = 2 << 16;

void *run(void *arg)
{
    //Add servers
    memcached_st *memc = memcached_create(NULL);
    memcached_server_st *servers = NULL;

    FILE *fin = fopen("nodes_ip.txt", "r");
    for (int i = 0; i < 2; i++)
    {
        char tmp[100] = {0};
        char ip[100] = {0};
        fgets(tmp, 100, fin);
        sscanf(tmp, "%s", ip);

        fprintf(stderr, "Link %s\n", tmp);
        for (int i = 0; i < NUM; i++)
        {
            printf("add memcached %s %u\n", ip, 22000 + i);
            servers = memcached_server_list_append(servers, ip, 22000 + i, &rc);
        }
    }
    fclose(fin);

    rc = memcached_server_push(memc, servers);
    if (rc == MEMCACHED_SUCCESS)
    {
        fprintf(stderr, "Added server successfully\n");
    }
    else
    {
        fprintf(stderr, "Couldn't add server: %s\n", memcached_strerror(memc, rc));
    }

    int ii = 1000;
    while (ii--)
    {
        //Set keys and values
        for (int i = 0; i < num_keys; i++)
        {
            memcached_set(memc, keys[i], strlen(keys[i]), values[i], strlen(values[i]), 3600, 0);
        }
        //get values
        char *getval;
        size_t val_len;
        uint32_t flags;
        for (int i = 0; i < num_keys; i++)
        {
            getval = memcached_get(memc, keys[i], strlen(keys[i]), &val_len, &flags, &rc);
            //printf("'key[%d]:%s'->'%s'\n", i, keys[i], getval);
        }
    }

    memcached_server_list_free(servers);
    memcached_free(memc);
}

//./back RACK Thread
int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        printf("./proxy RACK\n");
        exit(-1);
    }

    sscanf(argv[1], "%d", &rack);
    sscanf(argv[2], "%d", &thread_num);

    printf("Num Keys: %d   Key size: %d  value size: %d\n", num_keys, key_size, value_size);

    //Initilize keys and values. Here let all keys equal ?°bar?± for simplicity.
    keys = (char **)calloc(1, num_keys * sizeof(char **));
    values = (char **)calloc(1, num_keys * sizeof(char **));
    for (int i = 0; i < num_keys; i++)
    {
        keys[i] = (char *)calloc(1, key_size + 1);
        values[i] = (char *)calloc(1, value_size + 1);
        int j;
        for (j = 0; j < key_size; j++)
        {
            keys[i][j] = (char)(rand() % 26 + 97);
        }
        keys[i][j] = 0;
        // for(j = 0; j < value_size; j++)
        // {
        //     values[i][j] = (char) (rand() % 26 + 97);
        // }
        memset(values[i], i % 10 + '0', value_size * sizeof(char));
    }

    pthread_t id[32] = {0}; //max
    for (int i = 0; i < thread_num; i++)
    {
        pthread_create(&id[i], NULL, run, (void *)NULL);
    }

    for (int i = 0; i < thread_num; i++)
    {
        pthread_join(id[i], NULL);
    }

    //Delete keys
    for (int i = 0; i < num_keys; i++)
    {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(values);

    return 0;
}
