#include <libmemcached/common.h>

__thread uint32_t STRIPE_ID_INC = 0; //global stripe_id;

char *transfer_ustr_to_str(unsigned char *s, uint32_t len)
{
    uint32_t i = 0;
    char *p = (char *)malloc(sizeof(char) * len);
    for(i = 0; i < len; i++)
    {
        if(s[i] <= 127)
            p[i] = s[i];
        else
            p[i] = s[i] - 256;
    }
    return p;
}

unsigned char *transfer_str_to_ustr(char *s, uint32_t len)
{
    uint32_t i = 0;
    unsigned char *p = (unsigned char *)malloc(sizeof(char) * len);

    for(i = 0; i < len; i++)
    {
        if(s[i] >= 0)
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


void clear_encode_st(struct ECHash_st *ptr)
{
    int i;
    for(i = 0; i < N; i++)
    {
        if(ptr->encode.source_data[i])
            memset(ptr->encode.source_data[i], 0, CHUNK_SIZE * sizeof(unsigned char));
        else
            ptr->encode.source_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
    }

    for(i = 0; i < K; i++)
    {
        if(ptr->encode.left_data[i])
            memset(ptr->encode.left_data[i], 0, CHUNK_SIZE * sizeof(unsigned char));
        else
            ptr->encode.left_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
    }

    gf_gen_cauchy1_matrix(ptr->encode.encode_matrix, N, K);
    memset(ptr->encode.error_matrix, 0, N * K * sizeof(unsigned char));
    memset(ptr->encode.invert_matrix, 0, N * K * sizeof(unsigned char));
    memset(ptr->encode.decode_matrix, 0, N * K * sizeof(unsigned char));

    for(i = 0; i < N - K; i++)
    {
        if(ptr->encode.recovery_data[i])
            memset(ptr->encode.recovery_data[i], 0, CHUNK_SIZE * sizeof(unsigned char));
        else
            ptr->encode.recovery_data[i] = (unsigned char *)calloc(1, CHUNK_SIZE * sizeof(unsigned char));
    }
}

void destroy_encode_st(struct ECHash_st *ptr)
{
    int i;
    for(i = 0; i < N; i++)
    {
        free(ptr->encode.source_data[i]);
    }
    for(i = 0; i < K; i++)
    {
        free(ptr->encode.left_data[i]);
    }
    for(i = 0; i < N - K; i++)
    {
        free(ptr->encode.recovery_data[i]);
    }
}

inline static void cal_parity_id(uint32_t encode_data_ring[K], uint32_t encode_parity_ring[N - K])
{
    uint32_t i, j;
    uint32_t tmp[N] = {0};
    for(i = 0; i < K; i++)
    {
        tmp[encode_data_ring[i]] = 1;
    }
    for(i = 0, j = 0; i < N; i++)
    {
        //must be N-K
        if(tmp[i] == 0)
        {
            encode_parity_ring[j++] = i;
        }
    }
}

void encode(struct ECHash_st *ptr, uint32_t encode_data_ring[K], struct parity_kv kv[N - K], struct chunk_info_st tmp_cis[RING_SIZE])
{
    clear_encode_st(ptr);

    uint32_t i, j;

    for(i = 0; i < K; i++)
    {
        struct chunk_waitting_st *cws = chunk_waitting_pop(ptr, encode_data_ring[i]);
        memcpy(ptr->encode.source_data[i], cws->head, CHUNK_SIZE);

        tmp_cis[i].ring_id = encode_data_ring[i];
        tmp_cis[i].chunk_id = cws->chunk_id;
        tmp_cis[i].key = 0;

        chunk_list_set(ptr, cws, STRIPE_ID_INC);
    }

    //finish constructing the data,start encoding
    gf_gen_cauchy1_matrix(ptr->encode.encode_matrix, N, K);

    ec_init_tables(K, N - K, &(ptr->encode.encode_matrix[K * K]), ptr->encode.encode_gftbl);
    ec_encode_data(CHUNK_SIZE, K, N - K , ptr->encode.encode_gftbl, ptr->encode.source_data, &(ptr->encode.source_data[K]));

    //view the data
    // for(i=0;i<6;i++)
    // {
    //  printf("\n\nDATA%d\n\n",i);
    //  int j=0;
    //  for(j=0;j<CHUNK_SIZE;j++)
    //  {
    //      printf("%c",ptr->encode.source_data[i][j]);
    //  }
    // }

    uint32_t encode_parity_ring[4] = { -1, -1, -1, -1};
    cal_parity_id(encode_data_ring, encode_parity_ring);

    for(i = 0; i < N - K; i++)
    {
        kv[i].ring_id = encode_parity_ring[i];
        kv[i].stripe_id = STRIPE_ID_INC;

        //using the "striped_id-parity:ring_id" as the key
        char key_buff[256] = {0};
        int len = sprintf(key_buff, "stripe:%u-parity:%u", STRIPE_ID_INC, kv[i].ring_id);
        kv[i].key_length = len;
        kv[i].key = (char *)malloc(len * sizeof(char) + 1);
        memcpy(kv[i].key, key_buff, len);
        kv[i].key[len] = '\0';

        kv[i].value = transfer_ustr_to_str(ptr->encode.source_data[K + i], CHUNK_SIZE);

        uint64_t hash_value = create_value((uint64_t)len, (uint64_t)kv[i].stripe_id, (uint64_t)0, (uint64_t)CHUNK_SIZE);

        struct hash_node *hn = hash_node_init(kv[i].key, hash_value);
        insert_hash_table_key(ptr->rings[kv[i].ring_id].hash_table, hn, kv[i].key);
        printf("Parity_hash_node=>%s\n", display(hn));

    }

    for(i = K, j = 0; i < N; i++, j++)
    {
        tmp_cis[i].ring_id = kv[j].ring_id;
        tmp_cis[i].chunk_id = 0; //parity KV's chunk_id=0;
        tmp_cis[i].key = kv[j].key;
    }

    STRIPE_ID_INC++;
}

inline static int check_encode(struct ECHash_st *ptr, uint32_t encode_data_ring[K])
{
    //check here may overflow
    static uint32_t i = 0;
    uint32_t num = 0, k = 0;
    // for(i=0;i<K;i++)
    // {
    //  if(ptr->rings[encode_data_ring[i]].sealing_chunk_num<=0)
    //      return 0;//canot encode
    // }
    // return 1;//encode

    for(k = 0; k < RING_SIZE; i++, k++)
    {
        if(ptr->rings[i % RING_SIZE].sealing_chunk_num > 0)
        {
            encode_data_ring[num++] = i % RING_SIZE;
        }
        if(num == K)
            return 1;
    }
    return 0;
}

int try_encode(struct ECHash_st *ptr, struct parity_kv kv[N - K], struct chunk_info_st tmp_cis[RING_SIZE])
{
    uint32_t i = 0;
    uint32_t encode_data_ring[K] = {0};

    if(check_encode(ptr, encode_data_ring))
    {
        printf("\nData:");
        for(i = 0; i < K; i++)
        {
            printf("ring[%u]=%u;", encode_data_ring[i], ptr->rings[encode_data_ring[i]].sealing_chunk_num);
        }
        printf("Meet the encode requirements\n");
        encode(ptr, encode_data_ring, kv, tmp_cis);
        return 1;
    }
    return 0;
}

inline static int check_err(uint32_t index, uint32_t err_arr[N - K])
{
    uint32_t i = 0;
    for(i = 0; i < N - K; i++)
    {
        if(index == err_arr[i])
            return 1;
    }
    return 0;
}

void _decode(struct ECHash_st *ptr, struct gather_value gv[N], uint32_t err_arr[N - K])
{
    clear_encode_st(ptr);

    uint32_t i, j, k, r;
    for(i = 0, j = 0, k = 0; i < N; i++)
    {
        if(gv[i].ok == 1)
        {
            unsigned char *tmp_value = transfer_str_to_ustr(gv[i].value, CHUNK_SIZE);
            memcpy(ptr->encode.left_data[k++], tmp_value, CHUNK_SIZE);
            free(tmp_value);
            if(k == K && i < (N - 1))
            {
                //the last one was abandoned
                err_arr[j++] = i + 1;
                break;
            }
        }
        else
        {
            err_arr[j++] = i;
        }
    }

    for(i = 0, r = 0; i < N; i++)
    {
        if(check_err(i, err_arr))
            continue;
        for(j = 0; j < K; j++)
            ptr->encode.error_matrix[r * K + j] = ptr->encode.encode_matrix[i * K + j];
        r++;
    }

    gf_invert_matrix(ptr->encode.error_matrix, ptr->encode.invert_matrix, K);

    //construct the decode matrix
    int e = 0;
    for(e = 0; e < N - K; e++)
    {
        int idx = err_arr[e];
        if(idx < K)
        {
            for(j = 0; j < K; j++)
                ptr->encode.decode_matrix[e * K + j] = ptr->encode.invert_matrix[idx * K + j];
        }
        else
        {
            for(i = 0; i < K; i++)
            {
                unsigned char s = 0;
                for(j = 0; j < K; j++)
                    s ^= gf_mul(ptr->encode.invert_matrix[j * K + i], ptr->encode.encode_matrix[idx * K + j]);
                ptr->encode.decode_matrix[e * K + i] = s;
            }
        }
    }

    ec_init_tables(K, N - K, ptr->encode.decode_matrix, ptr->encode.decode_gftbl);
    ec_encode_data(CHUNK_SIZE, K, N - K , ptr->encode.decode_gftbl, ptr->encode.left_data, ptr->encode.recovery_data);

}

char *decode(struct ECHash_st *ptr, struct gather_value gv[N], uint32_t chunk_id)
{
    uint32_t err_arr[4] = {0};

    _decode(ptr, gv, err_arr);

    uint32_t i;
    for(i = 0; i < N - K; i++)
    {
        if(gv[err_arr[i]].cis->chunk_id == chunk_id)
            break;
    }
    printf("Get the failed chunk_id %u value,value={", gv[err_arr[i]].cis->chunk_id);

    char *ret = transfer_ustr_to_str(ptr->encode.recovery_data[i], CHUNK_SIZE);
    printf_arr(ret, CHUNK_SIZE);

    printf("}\n");

    return ret;
}

void _decode_parity(struct ECHash_st *ptr, struct gather_value gv[N], uint32_t err_arr[N - K])
{
    clear_encode_st(ptr);

    uint32_t i, j, k, r;
    for(i = 0, j = 0, k = 0; i < N && k < K; i++)
    {
        if(gv[i].ok == 1)
        {
            unsigned char *tmp_value = transfer_str_to_ustr(gv[i].value, CHUNK_SIZE);
            memcpy(ptr->encode.left_data[k++], tmp_value, CHUNK_SIZE);
            free(tmp_value);
        }
    }

    for(i = 0, r = 0; i < N; i++)
    {
        if(check_err(i, err_arr))
            continue;
        for(j = 0; j < K; j++)
            ptr->encode.error_matrix[r * K + j] = ptr->encode.encode_matrix[i * K + j];
        r++;
    }

    gf_invert_matrix(ptr->encode.error_matrix, ptr->encode.invert_matrix, K);

    int e = 0;
    for(e = 0; e < N - K; e++)
    {
        int idx = err_arr[e];
        if(idx < K)
        {
            for(j = 0; j < K; j++)
                ptr->encode.decode_matrix[e * K + j] = ptr->encode.invert_matrix[idx * K + j];
        }
        else
        {
            for(i = 0; i < K; i++)
            {
                unsigned char s = 0;
                for(j = 0; j < K; j++)
                    s ^= gf_mul(ptr->encode.invert_matrix[j * K + i], ptr->encode.encode_matrix[idx * K + j]);
                ptr->encode.decode_matrix[e * K + i] = s;
            }
        }
    }

    ec_init_tables(K, N - K, ptr->encode.decode_matrix, ptr->encode.decode_gftbl);
    ec_encode_data(CHUNK_SIZE, K, N - K , ptr->encode.decode_gftbl, ptr->encode.left_data, ptr->encode.recovery_data);

}

char *decode_parity(struct ECHash_st *ptr, struct gather_value gv[N], uint32_t ring_id)
{
    uint32_t err_arr[4] = {K, K + 1, K + 2, K + 3};

    _decode_parity(ptr, gv, err_arr);

    uint32_t i;
    for(i = 0; i < N - K; i++)
    {
        if(err_arr[i] >= K && gv[err_arr[i]].cis->ring_id == ring_id)
        {
            printf("Get the failed parity %s value,value={", gv[err_arr[i]].cis->key);
            break;
        }
    }
    printf("Get the failed parity %s value,value={", gv[err_arr[i]].cis->key);

    char *ret = transfer_ustr_to_str(ptr->encode.recovery_data[i], CHUNK_SIZE);
    printf_arr(ret, CHUNK_SIZE);

    printf("}\n");

    return ret;
}