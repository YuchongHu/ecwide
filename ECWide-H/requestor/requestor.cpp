#include "common.hpp"
#include "thread.hpp"

enum status
{
    conn_wait,
    conn_init,
    conn_write,
    conn_read,
    conn_close
};

int connfd_list[GROUP][RACK];

pthread_t connfd_list_mutex[GROUP][RACK];

int count_all = 0;
char **request;
struct threadpool *pool;
struct threadpool *repair;

static uint32_t op_load = 0, op_test = 0;
int count = 0;

static void request_init()
{
    // FILE *fin_para;
    // if(!(fin_para = fopen("para.txt", "r")))
    // {
    //     VERBOSE(4,"The para.txt open failed.\n");
    //     exit(-1);
    // }
    //char path[100]={0};
    // fscanf(fin_para,"Workloads Path=%s", path);

    char path1[100] = "ycsb_set.txt";
    char path2[100] = "ycsb_test.txt";
    //sprintf(path1, "ycsb_set.txt", path);
    //sprintf(path2, "ycsb_test.txt", path);

    FILE *fin_set, *fin_test;
    if (!(fin_set = fopen(path1, "r")))
    {
        VERBOSE(4, "The ycsb_set.txt open failed.\n");
        exit(-1);
    }

    if (!(fin_test = fopen(path2, "r")))
    {
        VERBOSE(4, "The ycsb_test.txt open failed.\n");
        exit(-1);
    }

    uint32_t load_set = 0, load_test = 0, get_test = 0;

    fscanf(fin_set, "Operationcount=%u", &op_load);
    fscanf(fin_test, "Operationcount=%u", &op_test);

    request = (char **)calloc(op_load + op_test, sizeof(char *));

    // for(uint32_t i = 0; i < op_load; i++)
    // {
    //     request[i] = (char *)malloc(sizeof(char));
    // }
    // for(uint32_t i = op_load; i < op_load + op_test; i++)
    // {
    //     request[i] = (char *)malloc(sizeof(char));
    // }

    char tmp[1024];
    uint32_t count = 0;
    fseek(fin_set, 0, SEEK_SET);

    while ((!feof(fin_set)) && fgets(tmp, 1024, fin_set))
    {

        char key[100] = {0};
        char value[1024] = {0};

        if (sscanf(tmp, "INSERT %s %[^\n]", key, value))
        {
            request[count] = (char *)malloc((strlen("set") + 1 + strlen(key) + 1 + strlen(value) + 1) * sizeof(char));
            strcat(request[count], "set ");
            strcat(request[count], key);
            strcat(request[count], " ");
            strcat(request[count], value);
            count++;
            //VERBOSE(4,"%s\n", request[count-1]);
        }
        else if (sscanf(tmp, "UPDATE %s %[^\n]", key, value))
        {
            request[count] = (char *)malloc((strlen("update") + 1 + strlen(key) + 1 + strlen(value) + 1) * sizeof(char));
            strcat(request[count], "update ");
            strcat(request[count], key);
            strcat(request[count], " ");
            strcat(request[count], value);
            count++;
        }
        else if (sscanf(tmp, "LOAD_INSERT=%u", &load_set))
        {
            ;
        }
    }

    fseek(fin_test, 0, SEEK_SET);
    while ((!feof(fin_test)) && fgets(tmp, 1024, fin_test))
    {
        char key[100] = {0};
        char value[1024] = {0};
        if (sscanf(tmp, "INSERT %s %[^\n]", key, value))
        {
            request[count] = (char *)malloc((strlen("set") + 1 + strlen(key) + 1 + strlen(value) + 1) * sizeof(char));
            strcat(request[count], "set ");
            strcat(request[count], key);
            strcat(request[count], " ");
            strcat(request[count], value);
            count++;
        }
        else if (sscanf(tmp, "READ %s", key))
        {
            request[count] = (char *)malloc((strlen("get") + 1 + strlen(key) + 1) * sizeof(char));
            strcat(request[count], "get ");
            strcat(request[count], key);
            count++;
        }
        else if (sscanf(tmp, "UPDATE %s %[^\n]", key, value))
        {
            request[count] = (char *)malloc((strlen("update") + 1 + strlen(key) + 1 + strlen(value) + 1) * sizeof(char));
            strcat(request[count], "update ");
            strcat(request[count], key);
            strcat(request[count], " ");
            strcat(request[count], value);
            count++;
        }
        else if (sscanf(tmp, "RUN_INSERT=%u", &load_test))
        {
        }
        else if (sscanf(tmp, "RUN_READ=%u", &get_test))
        {
        }
    }

    fclose(fin_set);
    fclose(fin_test);

    fprintf(stderr, "\nqueries_init...done\n\n");
}

//deal with KV
void *work(void *arg)
{
    char send_buf[CHUNK_SIZE];
    char receive_buf[CHUNK_SIZE];
    memset(send_buf, 0, CHUNK_SIZE);

    struct kv_arg *kk = (struct kv_arg *)arg;

    int fd = kk->fd;

    char op[10];
    sscanf(kk->kv, "%s", op);
    strcpy(send_buf, kk->kv);

    enum status status_now = conn_write;

    while (status_now != conn_close)
    {
        //char key[100];
        switch (status_now)
        {
        case conn_read:
        {
            //receive message
            memset(receive_buf, 0, CHUNK_SIZE);
            int ret = recv(fd, receive_buf, CHUNK_SIZE, 0);
            if (-1 == ret)
            {
                print_err("recv failed", errno);
            }
            else
            {
                VERBOSE(4, "\t[GET request ack]<={%s}\n", receive_buf);
            }

            status_now = conn_close;
            break;
        }
        case conn_write:
        {
            //send message
            //getchar();
            //scanf("%[^\n]",send_buf);
            //getchar();

            //VERBOSE(4,"s=%s\n",send_buf);
            int ret = send(fd, send_buf, CHUNK_SIZE, 0);
            if (-1 == ret)
                print_err("send failed", errno);
            else
                VERBOSE(4, "COUNT: %d, [SEND request]=>{kv}\n", kk->count);

            if (strncmp(op, "set", 3) == 0) //set
            {
                status_now = conn_read;
            }
            else if (strncmp(op, "get", 3) == 0) //get
            {
                status_now = conn_read;
            }
            else if (strncmp(op, "update", 6) == 0) //get
            {
                status_now = conn_read;
            }

            break;
        }
        case conn_close:
        {
            break;
        }
        }
    }
}

//distribute tasks
void *handle(void *args)
{
    while (count < op_load + 1)
    {
        struct kv_arg *kk = (struct kv_arg *)calloc(1, sizeof(struct kv_arg));
        kk->count = count;
        kk->kv = request[count];

        char key[100];
        //distributed by sum of key
        sscanf(request[count], "%*s %s", key);
        int sum = 0;
        for (int i = 0; i < strlen(key); i++)
        {
            sum = sum + key[i];
        }
        if (strcmp(key, "user6284781860667377211") == 0)
        {
            kk->fd = connfd_list[0][0];
        }
        else
        {
            printf("sum=%d, g=%d, r=%d\n", sum, (sum % (GROUP * RACK)) / RACK, (sum % (GROUP * RACK)) - RACK * ((sum % (GROUP * RACK)) / RACK));
            kk->fd = connfd_list[(sum % (GROUP * RACK)) / RACK][(sum % (GROUP * RACK)) - RACK * ((sum % (GROUP * RACK)) / RACK)];
        }

        threadpool_add_job(pool, work, (void *)kk);
        count++;
    }
    //exit(-1);
}

int main()
{
    //load memcached
    request_init();

    //thread pool
    pool = threadpool_init(1, 10);
    repair = threadpool_init(1, 10);

    int listenfd = -1, ret = -1;
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(reuse));

    if (-1 == listenfd)
    {
        print_err("Socket failed", errno);
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(S_PORT);
    addr.sin_addr.s_addr = inet_addr(S_IP);
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

    long connfd = -1;

    // pthread_t cid;
    // ret = pthread_create(&cid, NULL, connect_other, (void*)NULL);

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

        VERBOSE(4, "Cport = %d, caddr = %s\n", ntohs(caddr.sin_port), inet_ntoa(caddr.sin_addr));

        int gid, rid;
        char receive_buf[COMMAND_SIZE] = {0};
        ret = recv(connfd, receive_buf, COMMAND_SIZE, 0);
        if (-1 == ret)
        {
            print_err("recv failed", errno);
        }
        VERBOSE(4, "\t[GET connect]=>{%s}\n", receive_buf);
        if (strncmp(receive_buf, "CONN", 4) == 0)
        {
            sscanf(receive_buf, "%*s %d %d", &gid, &rid);
            VERBOSE(4, "\t[MANAGE CONN gid=%d, rid=%d]\n", gid, rid);
            connfd_list[gid][rid] = connfd;
            VERBOSE(4, "Accept: ");
            for (int i = 0; i < GROUP; i++)
            {
                for (int j = 0; j < RACK; j++)
                    VERBOSE(4, "%d, ", connfd_list[i][j]);
            }
            VERBOSE(4, "\n");
        }

        count_all++;

        if (count_all == GROUP * RACK)
        {
            for (int i = 0; i < 6; i++)
            {
                sleep(1);
                fprintf(stderr, "wait for proxy's eva %d s...\n", i);
            }
            pthread_t hid;
            int ret = pthread_create(&hid, NULL, handle, (void *)NULL);
        }
    }

    threadpool_destroy(pool);

    return 0;
}
