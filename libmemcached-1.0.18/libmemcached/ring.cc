#include <libmemcached/common.h>
#include "../libhashkit/common.h"
#include <sys/stat.h>
#include <math.h>

extern __thread uint32_t all_recovery_kv_parity, all_recovery_kv_data, all_for_parity_kv, all_for_data_kv, frag_recovery_kv_parity, frag_recovery_kv_data, frag_for_parity_kv, frag_for_data_kv;

extern __thread double get_in_scaling_parity, get_in_scaling_data;

__thread uint32_t get_once;
__thread uint32_t dget;
__thread uint32_t dget_success;
__thread uint32_t dget_failed;
__thread uint32_t dget_not_encode;

__thread double time_count_get = 0;
__thread double time_count_dget = 0;
__thread double time_parity_decode = 0;
__thread double time_data_decode = 0;
__thread double time_data_gather = 0;

__thread uint32_t KV_data_all = 0;
__thread uint32_t KV_parity_all = 0;

__thread uint32_t KV_data_ring_load[RING_SIZE] = {0};
__thread uint32_t KV_parity_ring_load[RING_SIZE] = {0};

__thread uint32_t KV_data_server_load[RING_SIZE][100] = {0};
__thread uint32_t KV_parity_server_load[RING_SIZE][100] = {0};
__thread int ring_flag = 0;

static int balance_cmp(const void *t1, const void *t2)
{
    struct balance_st *ct1 = (balance_st *)t1;
    struct balance_st *ct2 = (balance_st *)t2;

    /* Why 153? Hmmm... */
    WATCHPOINT_ASSERT(ct1->value != 153);
    if (ct1->value == ct2->value)
    {
        return 0;
    }
    else if (ct1->value > ct2->value)
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

static void run_balance(struct ECHash_st *ptr)
{
    uint32_t arr_index = 0;
    for(uint32_t ring_index = 0; ring_index < RING_SIZE; ring_index++)
    {
        for(uint32_t pointer_index = 0; pointer_index < RING_VIRTUAL; pointer_index++)
        {
            char sort_ring[100] = {0};
            uint32_t sort_ring_len = 0;

            sort_ring_len = sprintf(sort_ring, "%s:%u-%u", "Ring", ring_index, pointer_index);
            uint32_t value = hashkit_digest(&(ptr->rings[0].ring->hashkit), sort_ring, (size_t)sort_ring_len);
            ptr->balance_arr[arr_index].index = ring_index;
            ptr->balance_arr[arr_index++].value = value;

        }
    }

    qsort(ptr->balance_arr, RING_SIZE * RING_VIRTUAL, sizeof(struct balance_st), balance_cmp);

    printf("\nThe all ring virtual node:");
    for(uint32_t i = 0; i < RING_SIZE * RING_VIRTUAL; i++)
    {
        if(i % 10 == 0)
            printf("\n");
        printf("(%u,%u); ", ptr->balance_arr[i].index, ptr->balance_arr[i].value);
    }
    printf("\n\n");
}

memcached_return_t ECHash_init(struct ECHash_st **ptr)
{
    (*ptr) = (struct ECHash_st *)malloc(sizeof(struct ECHash_st)); //create the struct ECHash_st
    (*ptr)->total_num_server = 0;

    uint32_t i;
    for(i = 0; i < RING_SIZE; i++)
    {
        (*ptr)->rings[i].ring = memcached_create(NULL);
        memcached_behavior_set((*ptr)->rings[i].ring, MEMCACHED_BEHAVIOR_DISTRIBUTION, MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA);
        (*ptr)->rings[i].value_size = 0;
        chunk_waitting_init(*ptr, i);
    }

    (*ptr)->chunk_list_size = CHUNK_LIST_INIT;
    (*ptr)->chunk_list = (struct chunk_st *)calloc(CHUNK_LIST_INIT, sizeof(struct chunk_st));

    (*ptr)->stripe_list_size = STRIPE_LIST_INIT;
    (*ptr)->stripe_list = (struct chunk_info_st (*)[RING_SIZE])calloc(STRIPE_LIST_INIT, sizeof(struct chunk_info_st) * RING_SIZE);

    for(i = 0; i < RING_SIZE; i++)
    {
        hash_table_init((*ptr)->rings[i].hash_table);
    }

    for(i = 0; i < RING_SIZE; i++)
    {
        (*ptr)->rings[i].kv_set_waitting_list = 0;
    }

    (*ptr)->balance_arr = (struct balance_st *)calloc(1, RING_SIZE * RING_VIRTUAL * sizeof(struct balance_st));

    run_balance(*ptr);

    clear_encode_st(*ptr);

    //create the backup dir
    // if(access("/home/node/multi/libmemcached-1.0.18/config/", F_OK) != 1)
    // {
    //     mkdir("/home/node/multi/libmemcached-1.0.18/config/", 0777);
    // }
    // FILE *fin_ip_port = fopen("/home/node/multi/libmemcached-1.0.18/config/multi_ip_port.txt", "w+");
    // fprintf(fin_ip_port, "\n\n###The server list:\n");
    // fclose(fin_ip_port);

    // FILE *fin_affect = fopen("/home/node/multi/libmemcached-1.0.18/config/multi_affect.txt", "a");
    // fprintf(fin_affect, "\n\n##### The affect statistic #####\n");
    // fclose(fin_affect);

    printf("Rings init success.\n");
    return MEMCACHED_SUCCESS;
}


memcached_return_t ECHash_destroy(struct ECHash_st *ptr)
{
    uint32_t i;
    for(i = 0; i < RING_SIZE; i++)
    {
        memcached_free(ptr->rings[i].ring);
        chunk_waitting_destroy(ptr, i);
    }

    for(i = 0; i < ptr->chunk_list_size; i++)
    {
        struct chunk_st p = ptr->chunk_list[i];
        struct key_st *q = p.key_list, *k = q;
        while(q)
        {
            k = q;
            q = q->next;
            free(k);
        }
    }
    free(ptr->chunk_list);
    free(ptr->stripe_list);

    for(i = 0; i < RING_SIZE; i++)
    {
        hash_table_destory(ptr->rings[i].hash_table);
    }

    for(i = 0; i < RING_SIZE; i++)
    {
        struct kv_set_waitting_st *p = ptr->rings[i].kv_set_waitting_list, *q = p;
        while(p)
        {
            q = p;
            p = p->next;
            free(q->key);
            free(q->value);
            free(q);
        }
    }

    free(ptr->balance_arr);

    destroy_encode_st(ptr);

/*
    fprintf(stderr, "\nstripe=%u\n\n", STRIPE_ID_INC);

    fprintf(stderr, "\nAll_recovery_kv_parity=%u\nAll_recovery_kv_data=%u\nAll_for_parity_kv=%u\nAll_for_data_kv=%u\n\nfrag_recovery_kv_parity=%u\nfrag_recovery_kv_data=%u\nfrag_for_parity_kv=%u\n\
frag_for_data_kv=%u\n\ntime_data_decode=%f\ntime_parity_decode=%f\ntime_data_gather=%f\nget_in_scaling_parity=%f\nget_in_scaling_data=%f\n\n", all_recovery_kv_parity, all_recovery_kv_data, all_for_parity_kv, all_for_data_kv, frag_recovery_kv_parity, \
            frag_recovery_kv_data, frag_for_parity_kv, frag_for_data_kv, time_data_decode, time_parity_decode, time_data_gather, get_in_scaling_parity, get_in_scaling_data);

*/
    printf("Rings had been destroyed.\n");
    return MEMCACHED_SUCCESS;
}


uint32_t ECHash_stat_waitting_chunk(struct ECHash_st *ptr, uint32_t ring_id)
{
    uint32_t waitting_for_encode = 0;
    struct chunk_waitting_st *p = ptr->rings[ring_id].chunk_waitting_list;
    printf("***The chunk_waitting_list for ring(%u):waitting=%u,sealing=%u:\n", ring_id, ptr->rings[ring_id].waitting_length, ptr->rings[ring_id].sealing_chunk_num);

    while(p)
    {
        waitting_for_encode = waitting_for_encode + p->KV_num;
        printf("chunk_id=%u,KV_num=%u,chunk_used_size=%u,can_sealing=%u\n", p->chunk_id, p->KV_num, p->chunk_used_size, p->can_sealing);

        p = p->next;
    }
    printf("Ring(%u)==>waitting_for_encode=%u\n", ring_id, waitting_for_encode);
    return waitting_for_encode;
}

void ECHash_stat(struct ECHash_st *ptr, uint32_t stat[], double stat2[])
{
    stat[0] = get_once;
    stat[1] = dget;
    stat[2] = dget_success;
    stat[3] = dget_failed;
    stat[4] = dget_not_encode;
    // stat[5]=waitting_for_encode;

    stat2[0] = time_count_dget;

    printf("\n\nTime_count_get=%f  s.\n\n", time_count_get);
    printf("KV_data_ring_load for each ring:\n");

    for(uint32_t i = 0; i < RING_SIZE; i++)
    {
        printf("ring_id=%u==>%u(%d)\n", i, KV_data_ring_load[i], KV_data_ring_load[i] - KV_data_all / RING_SIZE);
    }

    printf("KV_data_ring_load for each server:\n");
    for(uint32_t i = 0; i < RING_SIZE; i++)
    {
        printf("In ring_id=%u\n", i);
        for(uint32_t j = 0; j < 100; j++)
        {
            if(KV_data_server_load[i][j] != 0)
            {
                printf("index=%u==>%u\n", j, KV_data_server_load[i][j]);
            }
        }
    }
    printf("KV_parity_ring_load for each ring:\n");
    for(uint32_t i = 0; i < RING_SIZE; i++)
    {
        printf("ring_id=%u==>%u(%d)\n", i, KV_parity_ring_load[i], KV_parity_ring_load[i] - KV_parity_all / RING_SIZE);
    }
    printf("\n");
    printf("KV_parity_ring_load for each server:\n");
    for(uint32_t i = 0; i < RING_SIZE; i++)
    {
        printf("In ring_id=%u\n", i);
        for(uint32_t j = 0; j < 100; j++)
        {
            if(KV_parity_server_load[i][j] != 0)
            {
                printf("index=%u==>%u\n", j, KV_parity_server_load[i][j], KV_parity_server_load[i][j]);
            }
        }
    }
}

//backup the metadata (ip_port & hashtable & chunklist &stripe_list)
/*void ECHash_backup(struct ECHash_st *ptr)
{
    time_t now;
    struct tm *tm;
    char dir_time[255] = {0};
    time(&now);
    tm = localtime(&now);
    strftime(dir_time, 255, "%F_%T", tm);

    char path[255] = {0};
    if(access("/home/node/multi/libmemcached-1.0.18/config/", F_OK) != 1)
    {
        mkdir("/home/node/multi/libmemcached-1.0.18/config/", 0777);
    }
    strcpy(path, "/home/node/multi/libmemcached-1.0.18/config/");
    strcat(path, dir_time);
    mkdir(path, 0777);

    uint32_t i = 0;
    for(i = 0; i < RING_SIZE; i++)
    {
        char file_hash_name[255] = {0};
        sprintf(file_hash_name, "%s/ring_id=%lu.txt", path, i);

        FILE *fin = fopen(file_hash_name, "w");
        uint32_t hash_inc = 0;
        for(hash_inc = 0; hash_inc < HASH_MAX_SIZE; hash_inc++)
        {
            char hash_line[1024] = {0};
            struct hash_node *hn = ptr->rings[i].hash_table[hash_inc];
            if(hn && hn->key)
            {
                sprintf(hash_line, " %d[BUCKET] %s %llu\n", hash_inc, hn->key, hn->value);
            }
            else
            {
                sprintf(hash_line, " %d[BUCKET]\n", hash_inc);
            }
            fputs(hash_line, fin);
        }
        fclose(fin);
    }

    char file_chunk_name[255] = {0};
    sprintf(file_chunk_name, "%s/%s", path, "chunk_list.txt");
    FILE *fin_chunk = fopen(file_chunk_name, "w");

    for(i = 0; i < CHUNK_ID_INC; i++)
    {
        char chunk_line[10240] = {0};
        fprintf(fin_chunk, " %lu[CHUNK] %d %lu %lu %lu\n", i, ptr->chunk_list[i].stat, ptr->chunk_list[i].stripe_id, ptr->chunk_list[i].used_size, ptr->chunk_list[i].KV_num);

        struct key_st *p = ptr->chunk_list[i].key_list;
        while(p)
        {
            if(p->hn && p->hn->key)
            {
                strcat(chunk_line, p->hn->key);
                strcat(chunk_line, " ");
            }
            else
            {
                strcat(chunk_line, "NULL ");
            }
            p = p->next;
        }

        strcat(chunk_line, "\n");
        fputs(chunk_line, fin_chunk);
    }
    fclose(fin_chunk);

    char file_stripe_name[255] = {0};
    sprintf(file_stripe_name, "%s/%s", path, "stripe_list.txt");
    FILE *fin_stripe = fopen(file_stripe_name, "w");

    for(i = 0; i < STRIPE_ID_INC; i++)
    {
        fprintf(fin_stripe, "%lu[STRIPE] ", i);

        int j = 0;
        for(j = 0; j < K; j++)
        {
            fprintf(fin_stripe, "%lu %lu ", ptr->stripe_list[i][j].ring_id, ptr->stripe_list[i][j].chunk_id);
        }
        for(j = K; j < N; j++)
        {
            fprintf(fin_stripe, "%lu %s ", ptr->stripe_list[i][j].ring_id, ptr->stripe_list[i][j].key);
        }

        fputs("\n", fin_stripe);
    }
    fclose(fin_stripe);
    printf("Backup over.\n");
}*/

static uint32_t get_ring_id(struct ECHash_st *ptr, const char *key, size_t key_length, int flag)
{
    if(flag == 0)
    {
        return libhashkit_md5( key, key_length) % RING_SIZE;
    }
    else
    {
        //any of memcached
        uint32_t hash = get_hash(ptr->rings[0].ring, key, key_length);
        uint32_t num = RING_SIZE * RING_VIRTUAL;
        WATCHPOINT_ASSERT(ptr->balance_arr);

        struct balance_st *begin, *end, *left, *right, *middle;
        begin = left = ptr->balance_arr;
        end = right = ptr->balance_arr + num;

        while (left < right)
        {
            middle = left + (right - left) / 2;
            if (middle->value < hash)
                left = middle + 1;
            else
                right = middle;
        }
        if (right == end)
            right = begin;
        return right->index;
    }
}

memcached_return_t ECHash_set(struct ECHash_st *ptr, const char *key, size_t key_length,
                                   const char *value, size_t value_length,
                                   time_t expiration,
                                   uint32_t  flags)
{
    memcached_return_t rc;
    uint32_t i;

    uint32_t ring_id = get_ring_id(ptr, key, key_length, ring_flag);
    KV_data_ring_load[ring_id]++;
    KV_data_all++;

    if(ptr->rings[ring_id].ring->in_migrating_lock)
    {
        struct kv_set_waitting_st *new_kv = (struct kv_set_waitting_st *)malloc(sizeof(struct kv_set_waitting_st));

        //head insert
        new_kv->next = ptr->rings[ring_id].kv_set_waitting_list;
        ptr->rings[ring_id].kv_set_waitting_list = new_kv;
        new_kv->key_length = key_length;
        new_kv->key = (char *)malloc(new_kv->key_length * sizeof(char) + 1);
        memcpy(new_kv->key, key, new_kv->key_length);
        new_kv->key[new_kv->key_length] = '\0';
        new_kv->value_length = value_length;
        new_kv->value = (char *)malloc(new_kv->value_length * sizeof(char) + 1);
        memcpy(new_kv->value, value, new_kv->value_length);
        new_kv->value[new_kv->value_length] = '\0';

    }
    else
    {
        rc = memcached_set(ptr->rings[ring_id].ring, key, key_length, value, value_length, expiration, flags);
        uint32_t server_key = memcached_generate_hash_with_redistribution(ptr->rings[ring_id].ring, key, key_length);
        KV_data_server_load[ring_id][server_key]++;
        if(rc == MEMCACHED_SUCCESS)
        {
            ptr->rings[ring_id].value_size += value_length;

            uint32_t chunk_id;
            //*set into chunk_waitting_list
            uint32_t position = chunk_waitting_set_kv(key, &chunk_id, ptr, ring_id, server_key, value, value_length);
            //construct hash_node
            uint64_t hash_value = create_value((uint64_t)key_length, (uint64_t)chunk_id, (uint64_t)position, (uint64_t)value_length);
            struct hash_node *hn = hash_node_init(key, hash_value);
            insert_hash_table_key(ptr->rings[ring_id].hash_table, hn, key);
            //set into the chunk_list
            chunk_list_append_key(ptr, chunk_id, hn);
            //encode check
            struct chunk_info_st tmp_cis[RING_SIZE];
            struct parity_kv kv[N - K];

            if(try_encode(ptr, kv, tmp_cis) == 0)
            {
                return rc;
            }
            else
            {
                for(i = 0; i < N - K; i++)
                {
                    memcached_return_t rc1 = memcached_set(ptr->rings[kv[i].ring_id].ring, kv[i].key, kv[i].key_length, kv[i].value, CHUNK_SIZE, 0, 0);
                    KV_parity_all++;
                    KV_parity_ring_load[kv[i].ring_id]++;

                    uint32_t server_key_1 = memcached_generate_hash_with_redistribution(ptr->rings[kv[i].ring_id].ring, kv[i].key, kv[i].key_length);
                    KV_parity_server_load[kv[i].ring_id][server_key_1]++;

                    if(rc1 == MEMCACHED_SUCCESS)
                    {
                        ptr->rings[kv[i].ring_id].value_size += CHUNK_SIZE;
                        printf("Finish set parity_chunk key[%s]\n", kv[i].key);
                    }
                    else
                    {
                        printf("Error set parity_chunk key[%s]\n", kv[i].key);
                        //exit(-1);
                    }
                }

                //view the paity
                // printf("\nValue=0\n");
                // for(i=0;i<CHUNK_SIZE;i++)
                // {
                //  printf("[%d] ",kv[0].value[i]);
                // }
                // printf("\nValue=1\n");
                // for(i=0;i<CHUNK_SIZE;i++)
                // {
                //  printf("[%d] ",kv[1].value[i]);
                // }

                //striped operation
                stripe_list_set(ptr, tmp_cis, kv[0].stripe_id);
            }
        }
        else
        {
            printf("SET ERROR.\n");
            return rc;
        }

        return rc;
    }

}

char *ECHash_dget(struct ECHash_st *ptr, const char *key, size_t key_length,
                       size_t *value_length,
                       uint32_t  *flags,
                       memcached_return_t *error)
{
    uint32_t ring_id = get_ring_id(ptr, key, key_length, ring_flag);
    dget++;

    char *value = 0;

    printf("\nGet [%s] failed,start dget\n", key);
    uint64_t hv = get_value_hash_table(ptr->rings[ring_id].hash_table, key);
    uint32_t chunk_id = GET_CHUNK_ID(hv);
    uint32_t position = GET_POSITION(hv);
    uint32_t length = GET_LENGTH(hv);

    printf("The failed key's hashtable info, hv=%lld,chunk_id=%u,position=%u,length=%u\n", hv, chunk_id, position, length);

    //whether the KV in the encode
    if(ptr->chunk_list[chunk_id].stat == Sealed)
    {
        uint32_t stripe_id = ptr->chunk_list[chunk_id].stripe_id;
        //construct the chunk info from the stripe_list
        struct gather_value gv[N];
        int err = gather_other_value(ptr, ring_id, 0, 0, stripe_id, gv);

        if(err)
        {
            printf("May occcur >2 server failed,cannot decode\n");
            dget_failed++;
            return NULL;
        }
        else
        {
            char *chunk_value = decode(ptr, gv, chunk_id);

            value = (char *)malloc(length * sizeof(char) + 1);
            memcpy(value, chunk_value + position, length);
            value[length] = '\0';
            *flags = 0;
            *value_length = length;
            free(chunk_value);
            dget_success++;
            printf("Dget [%s] success\n", key);
            return value;
        }
    }
    else if(ptr->chunk_list[chunk_id].stat == Sealed_repair)
    {
        printf("Key[%s] is in Sealed_repair\n", key);;
    }
    else
    {
        dget_not_encode++;
        printf("Key[%s] dont been encoded\n", key);
        return NULL;
    }

    return NULL;
}

char *ECHash_dget_data(struct ECHash_st *ptr, const char *key, size_t key_length,
                            memcached_return_t *error, int *f1, struct pos_len_st *pos_len_list, uint32_t count)
{
    //uint32_t ring_id=libhashkit_md5( key, key_length) % RING_SIZE;
    uint32_t ring_id = get_ring_id(ptr, key, key_length, ring_flag);
    char *value;

    printf("\nGet [%s] failed,start dget\n", key);

    for(uint32_t j = 0; j < count; j++)
    {
        printf("Pos_len_list[%u],pos=%u,len=%u\n", j, pos_len_list[j].pos, pos_len_list[j].len);
    }

    //get the stripe info
    uint64_t hv = get_value_hash_table(ptr->rings[ring_id].hash_table, key);
    uint32_t chunk_id = GET_CHUNK_ID(hv);
    //whether the KV in the encode
    if(ptr->chunk_list[chunk_id].stat == Sealed)
    {
        ptr->chunk_list[chunk_id].stat = Sealed_repair;

        uint32_t stripe_id = ptr->chunk_list[chunk_id].stripe_id;
        //construct the chunk info from the stripe_list
        struct gather_value gv[N];
        struct timeval begin, end;
        gettimeofday(&begin, NULL);
        int err = gather_other_value(ptr, ring_id, pos_len_list, count, stripe_id, gv);
        gettimeofday(&end, NULL);

        time_data_gather = time_data_gather + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

        if(err)
        {
            printf("May occcur >2 server failed,cannot decode\n");
            *f1 = -1;
            return NULL;
        }
        else
        {
            struct timeval begin, end;
            gettimeofday(&begin, NULL);
            char *chunk_value = decode(ptr, gv, chunk_id);

            value = (char *)malloc(CHUNK_SIZE * sizeof(char));
            memcpy(value, chunk_value, CHUNK_SIZE);

            free(chunk_value);
            printf("Dget [%s]'s chunk success\n", key);
            *f1 = 0;
            gettimeofday(&end, NULL);

            time_data_decode = time_data_decode + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
            return value;
        }
    }
    if(ptr->chunk_list[chunk_id].stat == Sealed_repair)
    {
        printf("Key[%s] have been repaired\n", key);
    }
    else
    {
        printf("Key[%s] dont been encoded\n", key);
        *f1 = 2;
        return NULL;
    }
}


char **ECHash_dget_data_batch(int batch, struct ECHash_st *ptr, const char **key, memcached_return_t *error, int *f1, struct pos_len_st (*pos_len_list)[100], uint32_t *count)
{
    int b = 0;
    uint32_t ring_id[BATCH_DATA] = { -1};
    uint32_t chunk_id[BATCH_DATA] = { -1};
    uint32_t stripe_id[BATCH_DATA] = { -1};
    struct gather_value gv[BATCH_DATA][N];
    char *value[BATCH_DATA];

    for(b = 0; b < batch; b++)
    {
        //uint32_t ring_id=libhashkit_md5( key, key_length) % RING_SIZE;
        ring_id[b] = get_ring_id(ptr, key[b], strlen(key[b]), ring_flag);

        printf("\nGet [%s] failed,start dget\n", key[b]);

        for(uint32_t j = 0; j < count[b]; j++)
        {
            printf("Pos_len_list[%u],pos=%u,len=%u\n", j, pos_len_list[b][j].pos, pos_len_list[b][j].len);
        }
        uint64_t hv = get_value_hash_table(ptr->rings[ring_id[b]].hash_table, key[b]);
        chunk_id[b] = GET_CHUNK_ID(hv);

        stripe_id[b] = ptr->chunk_list[chunk_id[b]].stripe_id;

    }
    printf("Batch check over, batch=%u\n", batch);

    struct timeval begin, end;
    gettimeofday(&begin, NULL);

    int *err = gather_other_value_batch(batch, ptr, ring_id, pos_len_list, count, stripe_id, gv);

    gettimeofday(&end, NULL);

    time_data_gather = time_data_gather + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;


    for(b = 0; b < batch; b++)
    {
        struct timeval begin, end;
        gettimeofday(&begin, NULL);

        char *chunk_value = decode(ptr, gv[b], chunk_id[b]);


        value[b] = (char *)malloc(CHUNK_SIZE * sizeof(char));
        memcpy(value[b], chunk_value, CHUNK_SIZE);

        free(chunk_value);
        printf("Dget [%s]'s chunk success\n", key[b]);
        *(f1 + b) = 0;

        gettimeofday(&end, NULL);

        time_data_decode = time_data_decode + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
    }

    return value;
}


char *ECHash_dget_parity(struct ECHash_st *ptr, const char *key, size_t key_length,
                              memcached_return_t *error)
{
    char *value = 0;

    printf("\nGet parity[%s] failed,start dget\n", key);

    uint32_t stripe_id, ring_id;
    sscanf(key, "stripe:%u-parity:%u", &stripe_id, &ring_id);

    struct gather_value gv[N];
    int err = gather_other_value(ptr, ring_id, 0, 0, stripe_id, gv);
    if(err)
    {
        printf("May occcur >2 server failed,cannot decode\n");
        return NULL;
    }
    else
    {
        struct timeval begin, end;
        gettimeofday(&begin, NULL);

        char *chunk_value = decode_parity(ptr, gv, ring_id);
        value = (char *)malloc(CHUNK_SIZE * sizeof(char));
        memcpy(value, chunk_value, CHUNK_SIZE);

        free(chunk_value);
        printf("Dget [%s] success\n", key);

        gettimeofday(&end, NULL);

        time_parity_decode = time_parity_decode + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
        return value;
    }
}


char **ECHash_dget_parity_batch(int batch, struct ECHash_st *ptr, const char **key, memcached_return_t *error)
{
    uint32_t b = 0;
    char *value[BATCH_PARITY];

    uint32_t stripe_id[BATCH_PARITY] = { -1};
    uint32_t ring_id[BATCH_PARITY] = { -1};
    struct gather_value gv[BATCH_PARITY][N];

    for(b = 0; b < batch; b++)
    {
        printf("\nGet parity[%s] failed,start dget\n", key[b]);
        sscanf(key[b], "stripe:%u-parity:%u", &(stripe_id[b]), &(ring_id[b]));
    }

    int *err = gather_other_value_batch_parity(batch, ptr, ring_id, stripe_id, gv);

    for(b = 0; b < batch; b++)
    {
        struct timeval begin, end;
        gettimeofday(&begin, NULL);

        char *chunk_value = decode_parity(ptr, gv[b], ring_id[b]);

        value[b] = (char *)malloc(CHUNK_SIZE * sizeof(char));
        memcpy(value[b], chunk_value, CHUNK_SIZE);
        //value[CHUNK_SIZE]='\0';
        free(chunk_value);
        printf("Dget [%s] success\n", key[b]);

        gettimeofday(&end, NULL);

        time_parity_decode = time_parity_decode + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
    }
    return value;
}


char *ECHash_get(struct ECHash_st *ptr, const char *key, size_t key_length,
                      size_t *value_length,
                      uint32_t  *flags,
                      memcached_return_t *error)
{
    uint32_t ring_id = get_ring_id(ptr, key, key_length, ring_flag);

    struct timeval begin, end;
    gettimeofday(&begin, NULL);
    char *value = memcached_get(ptr->rings[ring_id].ring, key, key_length, value_length, flags, error);
    gettimeofday(&end, NULL);

    time_count_get = time_count_get + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

    if(value )
    {
        get_once++;
        return value;
    }
    else
    {
        dget++;
        gettimeofday(&begin, NULL);

        int f1 = 0;
        struct pos_len_st pos_len_list[1] = {0};
        uint64_t hv = get_value_hash_table(ptr->rings[ring_id].hash_table, key);
        // uint32_t chunk_id = GET_CHUNK_ID(hv);
        pos_len_list[0].pos = GET_POSITION(hv);
        pos_len_list[0].len = GET_LENGTH(hv);

        value = ECHash_dget_data(ptr, key, key_length, error, &f1, pos_len_list, 1);

        if(f1 == 0)
        {
            dget_success++;
        }
        else if(f1 == -1)
        {
            dget_failed++;
        }
        else if(f1 == 2)
        {
            dget_not_encode++;
        }
        else
        {
            printf("not consider?\n");
        }

        if(value)
            *value_length = strlen(value);
        else
            *value_length = 0;
        gettimeofday(&end, NULL);
        time_count_dget = time_count_dget + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
        return value;
    }
}
