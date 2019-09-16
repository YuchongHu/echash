#include <libmemcached/common.h>
#include "../libhashkit/common.h"

memcached_return_t ECHash_init_addserver(struct ECHash_st *ptr,
        const char *hostname, in_port_t port, uint32_t ring_id)
{
    memcached_return_t rc;

    rc = memcached_server_add(ptr->rings[ring_id].ring, hostname, port);
    uint32_t sum = 0, i;
    for(i = 0; i < RING_SIZE; i++)
    {
        sum = sum + ptr->rings[i].ring->ketama.continuum_points_counter;
    }
    ptr->total_num_server = sum;

    return rc;
}

memcached_return_t ECHash_init_addserver_by_st(struct ECHash_st *ptr, struct server_add_st *st)
{
    memcached_return_t rc;
    rc = ECHash_init_addserver(ptr, st->hostname, st->port, st->ring_id);

    return rc;
}

memcached_return_t ECHash_init_addserver_by_list(struct ECHash_st *ptr, struct server_add_st *st[], int add_num)
{
    int add_num_inc = 0;
    for(add_num_inc = 0; add_num_inc < add_num; add_num_inc++)
    {
        ECHash_init_addserver_by_st(ptr, st[add_num_inc]);
    }

    return MEMCACHED_SUCCESS;
}

static inline int is_parity(char *s)
{
    //"stripe"
    char substr[6];
    memcpy(substr, s, 6);
    if(strcmp(substr, "stripe") == 0)
        return 1;
    return 0;
}

memcached_return_t ECHash_addserver(struct ECHash_st *ptr, const char *hostname, in_port_t port, uint32_t ring_id)
{
    memcached_return_t rc;

    rc = memcached_server_add_with_scale_out_lock(ptr->rings[ring_id].ring, hostname, port);

    if(rc == MEMCACHED_SUCCESS)
    {
        //Data migrating

        //cal the affect range, use hosts.cc inside solved
        //traverse the hash table,get the affect key
        printf("Server_add success,show out the affect key:\n");
        uint32_t count_data = 0;
        uint32_t count_parity = 0;

        double time_parity = 0;
        double time_data = 0;

        double time_set = 0;
        uint32_t count_set = 0;
        struct timeval set_begin, set_end;

        double time_get = 0;
        uint32_t count_get = 0;
        struct timeval get_begin, get_end;

        double time_del = 0;
        uint32_t count_del = 0;
        struct timeval del_begin, del_end;

        double time_reset = 0;
        uint32_t count_reset = 0;
        struct timeval reset_begin, reset_end;


        for(uint32_t i = 0; i < HASH_MAX_SIZE; i++)
        {
            struct hash_node *hn = ptr->rings[ring_id].hash_table[i];
            uint32_t k = 0;
            while(hn)
            {
                char *key = hn->key;
                uint32_t hash = 0;
                int index = if_affect_range(ptr->rings[ring_id].ring, key, strlen(key), &hash);

                if(index != -1)
                {
                    printf("[Affect]hash=%u,bucket=%u,migrate_from_index=%u,key_%u[%s]=>", hash, i, index, k, key);
                    if(is_parity(key))
                    {

                        struct timeval begin, end;
                        gettimeofday(&begin, NULL);

                        count_parity++;
                        size_t value_length = 0;
                        uint32_t flags = 0;

                        gettimeofday(&get_begin, NULL);
                        char *value = memcached_get_directly(ptr->rings[ring_id].ring, key, strlen(key), &value_length, &flags, &rc, index);

                        gettimeofday(&get_end, NULL);
                        time_get = time_get + (get_end.tv_sec - get_begin.tv_sec) + (double)(get_end.tv_usec - get_begin.tv_usec) / 1000000;
                        count_get++;

                        if(value)
                        {
                            gettimeofday(&set_begin, NULL);
                            memcached_return_t rc1 = memcached_set_in_scaling(ptr->rings[ring_id].ring, key, strlen(key), value, value_length, (time_t)0, (uint32_t)0);
                            gettimeofday(&set_end, NULL);
                            time_set = time_set + (set_end.tv_sec - set_begin.tv_sec) + (double)(set_end.tv_usec - set_begin.tv_usec) / 1000000;
                            count_set++;
                            if(rc1 == MEMCACHED_SUCCESS)
                                printf("MIGRATE parity[%s] to index=%u success\n", key, index);
                            else
                                printf("MIGRATE parity[%s] to index=%u failed\n", key, index);
                            gettimeofday(&del_begin, NULL);
                            rc1 = memcached_delete_directly(ptr->rings[ring_id].ring, key, strlen(key), (time_t)0, index);
                            gettimeofday(&del_end, NULL);
                            time_del = time_del + (del_end.tv_sec - del_begin.tv_sec) + (double)(del_end.tv_usec - del_begin.tv_usec) / 1000000;
                            count_del++;

                            if(rc1 == MEMCACHED_SUCCESS)
                                printf("DEL parity success\n");
                            else
                                printf("DEL parity failed\n");
                        }
                        else
                        {
                            printf("GET parity[%s] failed.\n", key);
                        }

                        gettimeofday(&end, NULL);
                        time_parity = time_parity + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

                    }
                    else//data,put it into kv_set_waitting_list
                    {
                        struct timeval begin, end;
                        gettimeofday(&begin, NULL);

                        count_data++;
                        struct kv_set_waitting_st *new_kv = (struct kv_set_waitting_st *)malloc(sizeof(struct kv_set_waitting_st));

                        //head insert
                        new_kv->next = ptr->rings[ring_id].kv_set_waitting_list;
                        ptr->rings[ring_id].kv_set_waitting_list = new_kv;
                        new_kv->key_length = strlen(key);
                        new_kv->key = (char *)malloc(new_kv->key_length * sizeof(char) + 1);
                        memcpy(new_kv->key, key, new_kv->key_length);
                        new_kv->key[new_kv->key_length] = '\0';
                        gettimeofday(&get_begin, NULL);
                        char *value = memcached_get_directly(ptr->rings[ring_id].ring, key, strlen(key), &(new_kv->value_length), &(new_kv->flags), &rc, index);

                        gettimeofday(&get_end, NULL);
                        time_get = time_get + (get_end.tv_sec - get_begin.tv_sec) + (double)(get_end.tv_usec - get_begin.tv_usec) / 1000000;
                        count_get++;

                        new_kv->value = (char *)malloc(new_kv->value_length * sizeof(char) + 1);
                        memcpy(new_kv->value, value, new_kv->value_length);
                        new_kv->value[new_kv->value_length] = '\0';

                        gettimeofday(&del_begin, NULL);
                        memcached_return_t rc1 = memcached_delete_directly(ptr->rings[ring_id].ring, new_kv->key, new_kv->key_length, (time_t)0, index);
                        gettimeofday(&del_end, NULL);
                        time_del = time_del + (del_end.tv_sec - del_begin.tv_sec) + (double)(del_end.tv_usec - del_begin.tv_usec) / 1000000;
                        count_del++;

                        if(rc1 == MEMCACHED_SUCCESS)
                            printf("DEL data[%s] success from index(%u)\n", new_kv->key, index);
                        else
                            printf("DEL data[%s] failed from index(%u)\n", new_kv->key, index);
                        gettimeofday(&end, NULL);
                        time_data = time_data + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
                    }
                }
                else
                {
                    //printf("Bucket=%d,key_%d[%s] is not affect\n",i,k,key);
                }

                //next
                k++;
                hn = hn->next;
            }
        }

        //judge whether have parity key
        memcached_in_scaling_unlock(ptr->rings[ring_id].ring);
        struct kv_set_waitting_st *p = ptr->rings[ring_id].kv_set_waitting_list, *q = p;
        while(p)
        {
            q = p;
            p = p->next;

            printf("MIGRATE data key[%s],ring_id=%u\n", q->key, ring_id);

            gettimeofday(&reset_begin, NULL);
            memcached_set(ptr->rings[ring_id].ring, q->key, q->key_length, q->value, q->value_length, (time_t)0, q->flags);
            gettimeofday(&reset_end, NULL);
            time_reset = time_reset + (reset_end.tv_sec - reset_begin.tv_sec) + (double)(reset_end.tv_usec - reset_begin.tv_usec) / 1000000;

            count_reset++;
            free(q->key);
            free(q->value);
            free(q);
        }
        ptr->rings[ring_id].kv_set_waitting_list = 0;
        fprintf(stderr, "ADD %s %u %u\ndata_key=%u,parity_key=%u\ntime_data=%f s,time_parity=%f s\ncount_set=%u,count_get=%u,count_del=%u,count_reset=%u\ntime_set=%f s,time_get=%f s,time_del=%f s,time_reset=%f s\n\n",
                hostname, port, ring_id, count_data, count_parity, time_data, time_parity, count_set, count_get, count_del, count_reset, time_set, time_get, time_del, time_reset);
        uint32_t sum = 0;
        for(uint32_t i = 0; i < RING_SIZE; i++)
        {
            sum = sum + ptr->rings[i].ring->ketama.continuum_points_counter;
        }
        ptr->total_num_server = sum;

        printf("After add_server success,sum=%u\n", sum);
        return rc;
    }
    else
    {
        memcached_in_scaling_unlock(ptr->rings[ring_id].ring);
        printf("ECHash_addserver failure.\n");
        return MEMCACHED_FAILURE;
    }
}

memcached_return_t ECHash_addserver_by_st(struct ECHash_st *ptr, struct server_add_st *st)
{
    memcached_return_t rc;
    rc = ECHash_addserver(ptr, st->hostname, st->port, st->ring_id);

    return rc;
}

memcached_return_t ECHash_addserver_by_list(struct ECHash_st *ptr, struct server_add_st *st[], int add_num)
{
    int add_num_inc = 0;
    for(add_num_inc = 0; add_num_inc < add_num; add_num_inc++)
    {
        memcached_return_t rc = ECHash_addserver_by_st(ptr, st[add_num_inc]);
    }

    return MEMCACHED_SUCCESS;
}

static inline memcached_instance_st *memcached_instance_get_by_hostname(struct ECHash_st *ptr, const char *hostname, in_port_t port, uint32_t ring_id, uint32_t *index)
{
    for (uint32_t i = 0; i < memcached_server_count(ptr->rings[ring_id].ring); i++)
    {
        //get the remove instance
        if(ptr->rings[ring_id].ring->servers[i].port_ == port && (strcmp(ptr->rings[ring_id].ring->servers[i]._hostname, hostname) == 0))
        {
            *index = i;
            memcached_instance_st *instance = memcached_instance_fetch(ptr->rings[ring_id].ring, i);
            printf("\n\nThe remove IP:PORT =>%s:%u\n", ptr->rings[ring_id].ring->servers[i]._hostname, ptr->rings[ring_id].ring->servers[i].port_);
            return instance;
        }
    }

    printf("The wrong IP or PORT.\n");
    return NULL;
}


memcached_return_t ECHash_remove_server(struct ECHash_st *ptr, const char *hostname, in_port_t port, uint32_t ring_id)
{
    //get the instance,index
    uint32_t remove_index = -1;
    memcached_instance_st *instance = memcached_instance_get_by_hostname(ptr, hostname, port, ring_id, &remove_index);

    instance->remove_flag = 1;
    printf("Remove_index=%u,ip:port=%s:%u,remove_flag=%d\n", remove_index, ptr->rings[ring_id].ring->servers[remove_index]._hostname, ptr->rings[ring_id].ring->servers[remove_index].port_, ptr->rings[ring_id].ring->servers[remove_index].remove_flag);

    memcached_return_t rc = run_distribution_with_scale_in_lock(ptr->rings[ring_id].ring);

    if(rc == MEMCACHED_SUCCESS)
    {
        printf("server_remove success,show out the affect key:\n");
        uint32_t count_data = 0;
        uint32_t count_parity = 0;

        double time_parity = 0;
        double time_data = 0;

        // uint32_t data_size = 0;
        double time_set = 0;
        uint32_t count_set = 0;
        struct timeval set_begin, set_end;

        double time_get = 0;
        uint32_t count_get = 0;
        struct timeval get_begin, get_end;


        double time_reset = 0;
        uint32_t count_reset = 0;
        struct timeval reset_begin, reset_end;


        for(uint32_t i = 0; i < HASH_MAX_SIZE; i++)
        {
            struct hash_node *hn = ptr->rings[ring_id].hash_table[i];
            uint32_t k = 0;
            while(hn)//have >1 element
            {
                char *key = hn->key;
                uint32_t hash = 0;
                int index = if_affect_range(ptr->rings[ring_id].ring, key, strlen(key), &hash);

                if(index != -1)
                {
                    printf("[Affect]hash=%u,bucket=%u,migrate_to_index=%u,key_%u[%s]=>\n", hash, i, index, k, key);

                    //parity,dont need set into the kv_set_waitting_list
                    if(is_parity(key))
                    {

                        struct timeval begin, end;
                        gettimeofday(&begin, NULL);

                        count_parity++;
                        size_t value_length = 0;
                        uint32_t flags = 0;

                        gettimeofday(&get_begin, NULL);
                        char *value = memcached_get_directly(ptr->rings[ring_id].ring, key, strlen(key), &value_length, &flags, &rc, remove_index);

                        gettimeofday(&get_end, NULL);
                        time_get = time_get + (get_end.tv_sec - get_begin.tv_sec) + (double)(get_end.tv_usec - get_begin.tv_usec) / 1000000;
                        count_get++;

                        if(value)
                        {
                            gettimeofday(&set_begin, NULL);
                            memcached_return_t rc1 = memcached_set_in_scaling(ptr->rings[ring_id].ring, key, strlen(key), value, value_length, (time_t)0, (uint32_t)0);
                            gettimeofday(&set_end, NULL);
                            time_set = time_set + (set_end.tv_sec - set_begin.tv_sec) + (double)(set_end.tv_usec - set_begin.tv_usec) / 1000000;
                            count_set++;

                            if(rc1 == MEMCACHED_SUCCESS)
                                printf("MIGRATE parity[%s] to index=%u success\n", key, index);
                            else
                                printf("MIGRATE parity[%s] to index=%u failed\n", key, index);
                        }
                        else
                        {
                            printf("GET parity[%s] failed.\n", key);
                        }

                        gettimeofday(&end, NULL);
                        time_parity = time_parity + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

                    }
                    else//data,put it into kv_set_waitting_list
                    {

                        struct timeval begin, end;
                        gettimeofday(&begin, NULL);

                        count_data++;
                        struct kv_set_waitting_st *new_kv = (struct kv_set_waitting_st *)malloc(sizeof(struct kv_set_waitting_st));

                        //head insert
                        new_kv->next = ptr->rings[ring_id].kv_set_waitting_list;
                        ptr->rings[ring_id].kv_set_waitting_list = new_kv;
                        new_kv->key_length = strlen(key);
                        new_kv->key = (char *)malloc(new_kv->key_length * sizeof(char) + 1);
                        memcpy(new_kv->key, key, new_kv->key_length);
                        new_kv->key[new_kv->key_length] = '\0';
                        gettimeofday(&get_begin, NULL);
                        char *value = memcached_get_directly(ptr->rings[ring_id].ring, key, strlen(key), &(new_kv->value_length), &(new_kv->flags), &rc, remove_index);
                        gettimeofday(&get_end, NULL);
                        time_get = time_get + (get_end.tv_sec - get_begin.tv_sec) + (double)(get_end.tv_usec - get_begin.tv_usec) / 1000000;

                        count_get++;

                        new_kv->value = (char *)malloc(new_kv->value_length * sizeof(char) + 1);
                        memcpy(new_kv->value, value, new_kv->value_length);
                        new_kv->value[new_kv->value_length] = '\0';

                        gettimeofday(&end, NULL);
                        time_data = time_data + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
                    }
                }
                else
                {
                    //printf("Bucket=%d,key_%d[%s] is not affect\n",i,k,key);
                }
                k++;
                hn = hn->next;
            }
        }

        memcached_in_scaling_unlock(ptr->rings[ring_id].ring);

        struct kv_set_waitting_st *p = ptr->rings[ring_id].kv_set_waitting_list, *q = p;
        while(p)
        {
            q = p;
            p = p->next;

            printf("MIGRATE data key[%s],ring_id=%u\n", q->key, ring_id);
            gettimeofday(&reset_begin, NULL);
            memcached_set(ptr->rings[ring_id].ring, q->key, q->key_length, q->value, q->value_length, (time_t)0, q->flags);
            gettimeofday(&reset_end, NULL);
            time_reset = time_reset + (reset_end.tv_sec - reset_begin.tv_sec) + (double)(reset_end.tv_usec - reset_begin.tv_usec) / 1000000;
            count_reset++;


            free(q->key);
            free(q->value);
            free(q);
        }
        ptr->rings[ring_id].kv_set_waitting_list = 0;

        fprintf(stderr, "REMOVE %s %u %u\ndata_key=%u,parity_key=%u\ntime_data=%f s,time_parity=%f s\ncount_set=%u,count_get=%u,count_reset=%u\ntime_set=%f s,time_get=%f s,time_reset=%f s\n\n",
                hostname, port, ring_id, count_data, count_parity, time_data, time_parity, count_set, count_get, count_reset, time_set, time_get, time_reset);

        uint32_t sum = 0;
        for(uint32_t i = 0; i < RING_SIZE; i++)
        {
            sum = sum + ptr->rings[i].ring->ketama.continuum_points_counter;
        }
        ptr->total_num_server = sum;

        printf("After remove_server sunccess,sum=%u\n", sum);
        return rc;
    }
    else
    {
        memcached_in_scaling_unlock(ptr->rings[ring_id].ring);
        printf("ECHash_remove_server failure.\n");
        return MEMCACHED_FAILURE;
    }
}

int if_mark(char *chunk_mark, struct hash_node *hn)
{
    uint32_t chunk_id = GET_CHUNK_ID(hn->value);
    if(chunk_mark[chunk_id] == 1)
        return 0;

    return -1;
}

memcached_return_t ECHash_repair_simulation(struct ECHash_st *ptr, const char *hostname, in_port_t port, uint32_t ring_id)
{
    uint32_t repair_index = -1;
    memcached_instance_st *instance = memcached_instance_get_by_hostname(ptr, hostname, port, ring_id, &repair_index);

    instance->remove_flag = 2;
    fprintf(stderr, "repair_index=%u,ip:port=%s:%u,remove_flag=%d\n", repair_index, ptr->rings[ring_id].ring->servers[repair_index]._hostname, ptr->rings[ring_id].ring->servers[repair_index].port_, ptr->rings[ring_id].ring->servers[repair_index].remove_flag);

    memcached_return_t rc = run_distribution_with_scale_in_lock(ptr->rings[ring_id].ring);

    if(rc == MEMCACHED_SUCCESS)
    {
        printf("server_failed success,show out the affect key:\n");
        uint32_t count_data = 0;
        uint32_t count_parity = 0;

        double time_parity = 0;
        double time_data = 0;

        double time_parity_dget = 0;
        double time_data_dget = 0;

        double time_parity_reset = 0;
        double time_data_reset = 0;

        uint32_t not_encode = 0;
        uint32_t err_over_2 = 0;
        int skip = 0;

        uint32_t count_reset = 0;

        double time_repair_pre = 0;
        double time_repair_pre_batch = 0;

        struct timeval begin, end;
        gettimeofday(&begin, NULL);

        struct chunk_repair_st *chunk_repair_list = NULL, *crl = chunk_repair_list;

        char parity_repair[3000][100] = {0};
        uint32_t parity_num = 0;

        char chunk_mark[1024 * 128] = {0};
        for(uint32_t i = 0; i < HASH_MAX_SIZE ; i++)
        {
            struct hash_node *hn = ptr->rings[ring_id].hash_table[i];
            struct timeval b, e;
            uint32_t k = 0;

            while(hn)//have >1 element
            {
                uint32_t hash = 0;
                char *key = hn->key;

                if(is_parity(key) && (if_affect_range(ptr->rings[ring_id].ring, key, strlen(key), &hash) != -1))
                {
                    count_parity++;
                    gettimeofday(&b, NULL);

                    strcpy(parity_repair[parity_num], key);
                    parity_num++;

                    printf("put parity_key[%s] into repair list\n", key);

                    gettimeofday(&e, NULL);

                    time_repair_pre_batch = time_repair_pre_batch + (e.tv_sec - b.tv_sec) + (double)(e.tv_usec - b.tv_usec) / 1000000;
                }
                else if((!is_parity(key)) && (if_mark(chunk_mark, hn) == -1) && (if_affect_range(ptr->rings[ring_id].ring, key, strlen(key), &hash) != -1))
                {
                    uint32_t chunk_id = GET_CHUNK_ID(hn->value);
                    chunk_mark[chunk_id] = 1;

                    uint32_t pos = GET_POSITION(hn->value);
                    uint32_t len = GET_LENGTH(hn->value);

                    printf("Traverse hashtable, key[%s],chunk_id=%u,pos=%u,len=%u\n", hn->key, chunk_id, pos, len);
                    crl = chunk_repair_list;

                    if(ptr->chunk_list[chunk_id].stat == Sealed && crl)
                    {
                        struct timeval b, e;
                        gettimeofday(&b, NULL);

                        struct chunk_repair_st *tmp = (struct chunk_repair_st *)calloc(1, sizeof(struct chunk_repair_st));
                        tmp->hns[tmp->count] = hn;
                        tmp->count++;
                        tmp->chunk_id = chunk_id;
                        tmp->next = chunk_repair_list;
                        printf("put it new for chunk_id=%u,%u\n", chunk_id, tmp->count - 1);
                        chunk_repair_list = tmp;

                        struct key_st *p = ptr->chunk_list[chunk_id].key_list;
                        while(p)
                        {
                            if(p->hn)
                            {
                                char *key = p->hn->key;
                                if(if_affect_range(ptr->rings[ring_id].ring, key, strlen(key), &hash) != -1)
                                {
                                    count_data++;
                                    tmp->hns[tmp->count] = p->hn;
                                    tmp->count++;
                                    tmp->chunk_id = chunk_id;
                                    printf("put it next for chunk_id=%u,%u\n", chunk_id, tmp->count - 1);

                                }

                                p = p->next;
                            }
                        }

                        gettimeofday(&e, NULL);

                        time_repair_pre_batch = time_repair_pre_batch + (e.tv_sec - b.tv_sec) + (double)(e.tv_usec - b.tv_usec) / 1000000;

                    }
                    else if(ptr->chunk_list[chunk_id].stat == Waitting)
                    {
                        not_encode++;
                        printf("Not encoded,neednot dget\n");
                    }
                    else
                    {
                        struct timeval b, e;
                        gettimeofday(&b, NULL);

                        struct chunk_repair_st *tmp = (struct chunk_repair_st *)calloc(1, sizeof(struct chunk_repair_st));
                        tmp->hns[tmp->count] = hn;
                        tmp->count++;
                        tmp->chunk_id = chunk_id;
                        tmp->next = NULL;
                        printf("put it first one for chunk_id=%u,%u\n", chunk_id, tmp->count - 1);
                        chunk_repair_list = tmp;

                        gettimeofday(&e, NULL);

                        time_repair_pre_batch = time_repair_pre_batch + (e.tv_sec - b.tv_sec) + (double)(e.tv_usec - b.tv_usec) / 1000000;
                    }
                }
                else
                {
                    ;
                }

                k++;
                hn = hn->next;
            }
        }
        gettimeofday(&end, NULL);

        time_repair_pre = time_repair_pre + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

        int batch = 0;
        uint32_t i = 0;
        printf("\nstart parity repair\n\n");
        gettimeofday(&begin, NULL);

        while(1)
        {
            char *key[BATCH_PARITY];
            batch = 0;

            if(i + BATCH_PARITY <= parity_num)
            {
                for(uint32_t j = 0; j < BATCH_PARITY; j++)
                    key[j] = parity_repair[i + j];

                char **value = ECHash_dget_parity_batch(BATCH_PARITY, ptr, (const char **)key, &rc);

                struct timeval b, e;
                gettimeofday(&b, NULL);

                for(uint32_t k = 0; k < BATCH_PARITY; k++)
                {
                    char tmp[CHUNK_SIZE];
                    memset(tmp, -1, CHUNK_SIZE);
                    memcached_return_t rc1 = memcached_set_in_scaling(ptr->rings[0].ring, parity_repair[i + k], strlen(parity_repair[i + k]), tmp, CHUNK_SIZE, (time_t)0, (uint32_t)0);
                    printf("MIGRATE parity[%s] success\n", parity_repair[i + k]);
                }

                i = i + 10;
                gettimeofday(&e, NULL);
                time_parity_reset = time_parity_reset + (e.tv_sec - b.tv_sec) + (double)(e.tv_usec - b.tv_usec) / 1000000;
            }
            else
            {
                for(uint32_t j = 0; j < parity_num - i; j++)
                    key[j] = parity_repair[i + j];

                // char **value = ECHash_dget_parity_batch(parity_num - i, ptr, (const char **)key, &rc);
                struct timeval b, e;
                gettimeofday(&b, NULL);

                for(uint32_t k = 0; k < parity_num - i; k++)
                {
                    char tmp[CHUNK_SIZE];
                    memset(tmp, -1, CHUNK_SIZE);
                    memcached_return_t rc1 = memcached_set_in_scaling(ptr->rings[0].ring, parity_repair[i + k], strlen(parity_repair[i + k]), tmp, CHUNK_SIZE, (time_t)0, (uint32_t)0);
                    printf("MIGRATE parity[%s] success\n", parity_repair[i + k]);
                }
                gettimeofday(&e, NULL);
                time_parity_reset = time_parity_reset + (e.tv_sec - b.tv_sec) + (double)(e.tv_usec - b.tv_usec) / 1000000;

                break;
            }
        }

        gettimeofday(&end, NULL);

        time_parity_dget = time_parity_dget + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;


        printf("\nStart data repair\n\n");

        gettimeofday(&begin, NULL);
        crl = chunk_repair_list;


        while(crl)
        {
            struct hash_node *hn[BATCH_DATA];
            char *key[BATCH_DATA];
            int f1[BATCH_DATA] = {0};
            struct pos_len_st pos_len_list[BATCH_DATA][100] = {0};
            uint32_t count[BATCH_DATA] = {0};

            batch = 0;

            while(crl && batch < BATCH_DATA)
            {
                hn[batch] = crl->hns[0];
                key[batch] = hn[batch]->key;
                for(uint32_t j = 0; j < crl->count; j++)
                {
                    struct hash_node *hn_t = crl->hns[j];
                    pos_len_list[batch][j].pos = GET_POSITION(hn_t->value);
                    pos_len_list[batch][j].len = GET_LENGTH(hn_t->value);
                }

                count[batch] = crl->count;

                batch++;
                crl = crl->next;
            }

            printf("BATCH=%u", batch);
            char **value = ECHash_dget_data_batch(batch, ptr, (const char **)key, &rc, f1, pos_len_list, count);
            while(--batch)
            {
                for(uint32_t j = 0; j < count[batch]; j++)
                {
                    uint32_t pos_t = GET_POSITION(hn[j]->value);
                    uint32_t len_t = GET_LENGTH(hn[j]->value);

                    struct kv_set_waitting_st *new_kv = (struct kv_set_waitting_st *)malloc(sizeof(struct kv_set_waitting_st));
                    //head insert
                    new_kv->next = ptr->rings[ring_id].kv_set_waitting_list;
                    ptr->rings[ring_id].kv_set_waitting_list = new_kv;
                    new_kv->key_length = strlen(hn[j]->key);
                    new_kv->key = (char *)malloc(new_kv->key_length * sizeof(char) + 1);
                    memcpy(new_kv->key, hn[j]->key, new_kv->key_length);
                    new_kv->key[new_kv->key_length] = '\0';
                    new_kv->value_length = len_t;

                    new_kv->flags = 0;
                    new_kv->value = (char *)malloc(new_kv->value_length * sizeof(char) + 1);
                    memcpy(new_kv->value, value[j] + pos_t, new_kv->value_length);
                    new_kv->value[new_kv->value_length] = '\0';
                    printf("put key[%s] into set waitting list.\n", new_kv->key);
                }
            }
        }

        gettimeofday(&end, NULL);
        time_data_dget = time_data_dget + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
        crl = chunk_repair_list;
        struct chunk_repair_st *tt = crl;
        while(crl)
        {
            skip = skip + crl->count - 1;
            tt = crl;
            crl = crl->next;
            free(tt);
        }
        memcached_in_scaling_unlock(ptr->rings[ring_id].ring);
        gettimeofday(&begin, NULL);
        struct kv_set_waitting_st *p = ptr->rings[ring_id].kv_set_waitting_list, *q = p;
        while(p)
        {
            q = p;
            p = p->next;
            memcached_set_directly(ptr->rings[ring_id].ring, q->key, q->key_length, q->value, q->value_length, (time_t)0, q->flags, repair_index);

            printf("Migrate key[%s] into cluster.\n", q->key);

            count_reset++;

            free(q->key);
            free(q->value);
            free(q);
        }
        ptr->rings[ring_id].kv_set_waitting_list = 0;

        gettimeofday(&end, NULL);

        time_data_reset = time_data_reset + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;
        fprintf(stderr, "REPAIR %s %u %u\ndata_key=%u,parity_key=%u\ntime_data=%f s,time_parity=%f s\nnot_encode=%u,err_over_2=%u,skip=%d\ncount_reset=%u\ntime_repair_pre=%f, time_repair_pre_batch=%f, time_parity_dget=%f \
time_data_dget=%f,time_parity_reset=%f,time_data_reset=%f\n\n", hostname, port, ring_id, count_data, count_parity, (double)time_data / CLOCKS_PER_SEC, (double)time_parity / CLOCKS_PER_SEC, not_encode, err_over_2, skip, count_reset, \
                time_repair_pre, time_repair_pre_batch, time_parity_dget - time_parity_reset, time_data_dget, time_parity_reset, time_data_reset);

        for(uint32_t chunk_inc = 0; chunk_inc < ptr-> chunk_list_size; chunk_inc++)
        {
            if(ptr->chunk_list[chunk_inc].stat == Sealed_repair)
                ptr->chunk_list[chunk_inc].stat = Sealed;
        }

        uint32_t sum = 0;
        for(uint32_t i = 0; i < RING_SIZE; i++)
        {
            sum = sum + ptr->rings[i].ring->ketama.continuum_points_counter;
        }
        ptr->total_num_server = sum;
        printf("After repair_server success,sum=%u\n", sum);
        return rc;
    }
    else
    {
        memcached_in_scaling_unlock(ptr->rings[ring_id].ring);
        printf("ECHash_repair_server failure.\n");
        return MEMCACHED_FAILURE;
    }
}