#include "encode.hpp"

//unsigned char encode_gftbl[32 * LK * (LN - LK)];
//unsigned char encode_matrix[LN * LK];

char *transfer_ustr_to_str(unsigned char *s, uint32_t len)
{
    char *p = (char *)malloc(sizeof(char) * len);
    for (uint32_t i = 0; i < len; i++)
    {
        if (s[i] <= 127)
            p[i] = s[i];
        else
            p[i] = s[i] - 256;
    }
    return p;
}

unsigned char *transfer_str_to_ustr(char *s, uint32_t len)
{
    unsigned char *p = (unsigned char *)malloc(sizeof(char) * len);

    for (uint32_t i = 0; i < len; i++)
    {
        if (s[i] >= 0)
        {
            p[i] = s[i];
        }
        else
        {
            p[i] = s[i] + 256;
        }
    }
    return p;
}

void show_local(unsigned char *parity)
{
    VERBOSE(2, "\n**********Local Parity start**********\n");
    int r = 0, i = 0;
    for (int i = 0; i < CHUNK_SIZE;)
    {
        while (parity[i + r] == parity[i] && i + r < CHUNK_SIZE)
        {
            r++;
        }
        VERBOSE(2, "[%d] for %d times;\n", parity[i], r);
        i = i + r;
        r = 0;
    }
    VERBOSE(2, "**********Local Parity end**********\n");
}

void show_global(unsigned char **parity)
{
    VERBOSE(2, "\n**********Global Parity start**********\n");
    for (int i = 0; i < GN - GK; i++)
    {
        VERBOSE(2, "\n***g-%d head***\n", i);
        int r = 0, j = 0;
        for (int j = 0; j < CHUNK_SIZE;)
        {
            while (parity[i][j + r] == parity[i][j] && j + r < CHUNK_SIZE)
            {
                r++;
            }
            VERBOSE(2, "[%d] for %d times;\n", parity[i][j], r);
            j = j + r;
            r = 0;
        }
        VERBOSE(2, "***g-%d tail***\n", i);
    }

    VERBOSE(2, "\n**********Global Parity end**********\n");
}

void show_data(char *data, int len, const char *s)
{
    VERBOSE(4, "\n**********%s start**********\n", s);
    int r = 0, i = 0;
    for (i = 0; i < len;)
    {
        while (data[i + r] == data[i] && i + r < len)
        {
            r++;
        }
        VERBOSE(4, "[%c] for %d times;\n", data[i], r);
        i = i + r;
        r = 0;
    }

    VERBOSE(4, "**********%s end**********\n", s);
}

void show_unsigned_data(unsigned char *data, int len, const char *s)
{
    VERBOSE(4, "\n**********%s start**********\n", s);
    int r = 0, i = 0;
    for (int i = 0; i < CHUNK_SIZE;)
    {
        r = 0;
        while (data[i + r] == data[i] && i + r < CHUNK_SIZE)
        {
            r++;
        }
        VERBOSE(4, "[%d] for %d times;\n", data[i], r);
        i = i + r;
        r = 0;
    }
    VERBOSE(4, "*********%s end**********\n", s);
}

unsigned char *l_encode(struct local_encode_st *encode)
{
    unsigned char encode_gftbl[32 * LK * (LN - LK)];
    unsigned char encode_matrix[LN * LK];

    gf_gen_rs_matrix(encode_matrix, LN, LK);

    // printf("encode_matrix:\n");
    // for(int i=0;i<LN*LK;)
    // {
    //     printf("%3d ",encode_matrix[i]);
    //     i++;
    //     if(i%LK==0)
    //         printf("\n");
    // }

    ec_init_tables(LK, LN - LK, &(encode_matrix[LK * LK]), encode_gftbl);
    ec_encode_data(CHUNK_SIZE, LK, LN - LK, encode_gftbl, encode->source_data, &(encode->source_data[LK]));

    // for(int i=0;i<LK;i++)
    // {
    //     //if(encode->source_data[i])
    //     //    printf("i=%d is OK\n",i);
    //     show_data(transfer_ustr_to_str(encode->source_data[i],CHUNK_SIZE),CHUNK_SIZE, "Local data");
    // }

    unsigned char *parity = encode->source_data[LK];
    show_local(parity);

    return parity;
}

unsigned char **g_encode(struct global_encode_st *encode)
{
    unsigned char encode_gftbl[32 * GK * (GN - GK)];
    unsigned char encode_matrix[GN * GK];

    // printf("encode_matrix:\n");
    // for(int i=0;i<GN*GK;)
    // {
    //     printf("%3d ",encode_matrix[i]);
    //     i++;
    //     if(i%GK==0)
    //         printf("\n");
    // }

    gf_gen_cauchy1_matrix(encode_matrix, GN, GK);

    ec_init_tables(GK, GN - GK, &(encode_matrix[GK * GK]), encode_gftbl);
    ec_encode_data(CHUNK_SIZE, GK, GN - GK, encode_gftbl, encode->source_data, &(encode->source_data[GK]));

    // for(int i=0;i<GK;i++)
    // {
    //     //if(encode->source_data[i])
    //      //  printf("i=%d is OK\n",i);
    //     show_data(transfer_ustr_to_str(encode->source_data[i],CHUNK_SIZE),CHUNK_SIZE, "Global data");
    // }

    unsigned char **parity = &(encode->source_data[GK]);
    show_global(parity);

    return parity;
}

//local repair middle result
unsigned char *l_middle(unsigned char **data, int count)
{
    int k = count;
    int n = count + 1;
    unsigned char encode_gftbl[32 * k * (n - k)];
    unsigned char encode_matrix[n * k];

    gf_gen_rs_matrix(encode_matrix, n, k);

    ec_init_tables(k, n - k, &(encode_matrix[k * k]), encode_gftbl);
    ec_encode_data(CHUNK_SIZE, k, n - k, encode_gftbl, data, &(data[k]));

    for (int i = 0; i < k; i++)
    {
        //if(encode->source_data[i])
        //    printf("i=%d is OK\n",i);
        show_unsigned_data(data[i], CHUNK_SIZE, "Middle data");
    }

    unsigned char *middle = data[k];
    show_unsigned_data(middle, CHUNK_SIZE, "Middle");

    return middle;
}

unsigned char *l_decode(unsigned char **data, unsigned char *recovery, int need)
{
    int k = need;
    int n = need + 1;

    unsigned char encode_gftbl[32 * k * (n - k)];
    unsigned char encode_matrix[n * k];

    gf_gen_rs_matrix(encode_matrix, n, k);

    // printf("decode_matrix:\n");
    // for(int i=0;i<n*k;)
    // {
    //     printf("%3d ",encode_matrix[i]);
    //     i++;
    //     if(i%k==0)
    //         printf("\n");
    // }

    ec_init_tables(k, n - k, &(encode_matrix[k * k]), encode_gftbl);
    ec_encode_data(CHUNK_SIZE, k, n - k, encode_gftbl, data, &(recovery));

    for (int i = 0; i < k; i++)
    {
        //if(encode->source_data[i])
        //    printf("i=%d is OK\n",i);
        show_unsigned_data(data[i], CHUNK_SIZE, "Repair data");
    }

    //show recovery data
    show_unsigned_data(recovery, CHUNK_SIZE, "Recovery");

    //printf("%s", transfer_ustr_to_str(recovery,CHUNK_SIZE));

    return recovery;
}