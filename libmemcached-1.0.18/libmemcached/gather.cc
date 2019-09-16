#include "libmemcached/common.h"

void printf_arr(char *s, uint32_t len)
{
    uint32_t i;
    for(i = 0; i < len; i++)
    {
        printf("%c", s[i]);
    }
}

__thread uint32_t all_recovery_kv_parity = 0, all_recovery_kv_data = 0, all_for_parity_kv = 0, all_for_data_kv = 0, frag_recovery_kv_parity = 0, frag_recovery_kv_data = 0, frag_for_parity_kv, frag_for_data_kv = 0;

__thread double get_in_scaling_parity = 0, get_in_scaling_data = 0;

static inline int check_frag_range(struct pos_len_st *pos_len_list, uint32_t count, uint32_t pos, uint32_t len)
{
    if(count == 0)
    {
        return 1;
    }
    else
    {
        for(uint32_t i = 1; i < count; i++)
        {
            uint32_t position = pos_len_list[i].pos;
            uint32_t length = pos_len_list[i].len;

            if(pos >= position && pos <= (position + length - 1))
                return 1;
            if((pos + len - 1) >= position && (pos + len - 1) <= (position + length - 1))
                return 1;
        }

        return 0;
    }
}

static int get_value_from_chunk(struct ECHash_st *ptr, struct pos_len_st *pos_len_list, uint32_t count, struct gather_value *gv)
{
    gv->value = (char *)calloc(1, CHUNK_SIZE * sizeof(char));
    uint32_t ring_id = gv->cis->ring_id;

    size_t value_length;
    uint32_t  flags;
    memcached_return_t rc;
    if(gv->cis->key)
    {
        //stripe:%u-parity:%u
        struct memcached_st *tmp = ptr->rings[ring_id].ring;
        struct timeval begin, end;
        gettimeofday(&begin, NULL);
        char *value = memcached_get_in_scaling(tmp, gv->cis->key, strlen(gv->cis->key), &value_length, &flags, &rc);
        gettimeofday(&end, NULL);

        get_in_scaling_parity = get_in_scaling_parity + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

        all_recovery_kv_parity++;
        frag_recovery_kv_parity++;
        if(value)
        {
            gv->ok = 1;
            memcpy(gv->value, value, value_length);
            printf("In pthread[%u,PARITY]====================>OK\n", gv->cis->ring_id);
            return 0;
        }
        else
        {
            gv->ok = 0;
            free(gv->value);
            printf("In pthread[%u,PARITY]====================>NOT OK\n", gv->cis->ring_id);
            return 1;
        }
    }
    else//data
    {
        uint32_t chunk_id = gv->cis->chunk_id;
        struct key_st *p = ptr->chunk_list[chunk_id].key_list;
        char *key;

        while(p)
        {
            if(p->hn)
            {
                key = p->hn->key;

                //stripe:%u-parity:%u
                uint32_t pos = GET_POSITION(p->hn->value);
                uint32_t len = GET_LENGTH(p->hn->value);

                if(count == 0)
                {
                    all_for_parity_kv++;
                }
                else
                {
                    all_for_data_kv++;
                }
                all_recovery_kv_data++;

                if(check_frag_range(pos_len_list, count, pos, len))
                {
                    struct memcached_st *tmp = ptr->rings[ring_id].ring;
                    struct timeval begin, end;
                    gettimeofday(&begin, NULL);
                    char *value = memcached_get_in_scaling(tmp, key, strlen(key), &value_length, &flags, &rc);
                    gettimeofday(&end, NULL);

                    get_in_scaling_data = get_in_scaling_data + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

                    if(count == 0)
                    {
                        frag_for_parity_kv++;
                    }
                    else
                    {
                        frag_for_data_kv++;
                    }
                    frag_recovery_kv_data++;

                    if(value)
                    {
                        printf("*get key[%s],at pos=%u, len=%u ==>{ok}\n", key, pos, len);
                        memcpy(gv->value + pos, value, value_length);
                    }
                    else
                    {
                        printf("*get key[%s]==>{not ok}\n", key);
                        gv->ok = 0;
                        printf("In pthread[%u,DATA],chunk_id=%u====================>NOT OK\n", gv->cis->ring_id, chunk_id);
                        free(gv->value);
                        return 1;
                    }
                }
            }
            p = p->next;
        }

        printf("In pthread[%u,DATA],chunk_id=%u====================>OK\n", gv->cis->ring_id, chunk_id);
        gv->ok = 1;
        return 0;
    }
}

int gather_other_value(struct ECHash_st *ptr, uint32_t ring_id, struct pos_len_st *pos_len_list, uint32_t count, uint32_t stripe_id, struct gather_value *gv)
{
    uint32_t i = 0;
    for(i = 0; i < N; i++)
    {
        gv[i].cis = &(ptr->stripe_list[stripe_id][i]);
        gv[i].ok = 0;
    }
    int ok = 0;

    for(i = 0; i < N; i++)
    {
        if(gv[i].cis->ring_id == ring_id)
            continue;

        get_value_from_chunk(ptr, pos_len_list, count, &gv[i]);

        if(gv[i].ok)
        {
            //view the value data
            // printf("Gather %d value success.{\n",gv[i].cis->ring_id);
            // uint32_t j;
            // for(j=0;j<CHUNK_SIZE;j++)
            // {
            //  printf("%c ",gv[i].value[j]);
            // }
            // printf("}\n");
            ok++;

            if(ok == K)
                break;
        }
    }
    if(ok >= K)
        return 0;
    else
        return 1;
}

//in order gather_batch
static int gather_batch_cmp(const void *t1, const void *t2)
{
    struct gather_batch *gb1 = (struct gather_batch *)t1;
    struct gather_batch *gb2 = (struct gather_batch *)t2;

    if(gb1->ring_id < gb2->ring_id)
        return -1;
    else if(gb1->ring_id > gb2->ring_id)
        return 1;
    else
    {
        if(gb1->index < gb2->index)
            return -1;
        else if(gb1->index > gb2->index)
            return 1;
        else
            return 0;
    }
}

static void get_batch(struct ECHash_st *ptr, struct gather_batch *gb, uint32_t num)
{
    qsort(gb, num, sizeof(struct gather_batch), gather_batch_cmp);

    for(uint32_t i = 0; i < num; i++)
    {
        printf("ring_id=%u,index=%u,key=%s\n", gb[i].ring_id, gb[i].index, gb[i].key);
    }


    for(uint32_t i = 0; i < num; i++)
    {
        size_t value_length;
        uint32_t  flags;
        memcached_return_t rc;

        if(gb[i].key)
        {
            struct memcached_st *tmp = ptr->rings[gb[i].ring_id].ring;
            struct timeval begin, end;
            gettimeofday(&begin, NULL);

            char *value = memcached_get_in_scaling(tmp, gb[i].key, strlen(gb[i].key), &value_length, &flags, &rc);
            gettimeofday(&end, NULL);

            get_in_scaling_parity = get_in_scaling_parity + (end.tv_sec - begin.tv_sec) + (double)(end.tv_usec - begin.tv_usec) / 1000000;

            if(value)
            {
                memcpy(gb[i].pos, value, value_length);
                printf("get[%s],fill into pos=[%p],value_length=%zu====================>ok\n", gb[i].key, gb[i].pos, value_length);
            }
            else
            {
                printf("get[%s],fill into pos=[%p],value_length=%zu====================>ok\n", gb[i].key, gb[i].pos, value_length);
            }
        }
    }
}


static void get_value_from_chunk_batch(struct ECHash_st *ptr, struct pos_len_st *pos_len_list, uint32_t count, struct gather_value *gv, struct gather_batch *gb, uint32_t *num)
{
    gv->value = (char *)calloc(1, CHUNK_SIZE * sizeof(char));
    uint32_t ring_id = gv->cis->ring_id;
    //parity
    if(gv->cis->key)
    {
        //stripe:%u-parity:%u
        struct memcached_st *tmp = ptr->rings[ring_id].ring;
        uint32_t index = memcached_generate_hash_with_redistribution(tmp, gv->cis->key, strlen(gv->cis->key));

        gb[*num].ring_id = ring_id;
        gb[*num].index = index;
        strcpy(gb[*num].key, gv->cis->key);
        gb[*num].pos = gv->value;

        printf("[Data]Num=%u,ring_id=%u,index=%u,key[%s],pos=%p\n", *num, gb[*num].ring_id, gb[*num].index, gb[*num].key, gb[*num].pos);
        (*num)++;
        all_recovery_kv_parity++;
        frag_recovery_kv_parity++;

        gv->ok = 1;

    }
    else//data
    {
        uint32_t chunk_id = gv->cis->chunk_id;
        struct key_st *p = ptr->chunk_list[chunk_id].key_list;
        //printf("In pthread[%d,DATA],chunk_id=%d,p=%p ",gv->cis->ring_id,chunk_id,p);
        char *key;

        while(p)
        {
            if(p->hn)
            {
                key = p->hn->key;

                //stripe:%u-parity:%u
                uint32_t pos = GET_POSITION(p->hn->value);
                uint32_t len = GET_LENGTH(p->hn->value);

                all_for_data_kv++;
                all_recovery_kv_data++;
                if(check_frag_range(pos_len_list, count, pos, len))
                {
                    struct memcached_st *tmp = ptr->rings[ring_id].ring;
                    uint32_t index = memcached_generate_hash_with_redistribution(tmp, key, strlen(key));

                    gb[*num].ring_id = ring_id;
                    gb[*num].index = index;
                    strcpy(gb[*num].key, key);
                    gb[*num].pos = gv->value + pos;


                    printf("[Data]Num=%u,ring_id=%u,index=%u,key[%s],pos=%p\n", *num, gb[*num].ring_id, gb[*num].index, gb[*num].key, gb[*num].pos);

                    (*num)++;

                    frag_for_data_kv++;

                    frag_recovery_kv_data++;
                }
            }
            p = p->next;
        }
        gv->ok = 1;
    }
}


int *gather_other_value_batch(int batch, struct ECHash_st *ptr, uint32_t *ring_id, struct pos_len_st (*pos_len_list)[100], uint32_t *count, uint32_t *stripe_id, struct gather_value (*gv)[N])
{
    int *err = (int *)malloc(batch * sizeof(int));
    uint32_t num = 0;
    struct gather_batch gb[BUFFER_DATA] = {0};

    for(uint32_t i = 0; i < BUFFER_DATA; i++)
    {
        gb[i].key[0] = '\0';
    }

    for(int b = 0; b < batch; b++)
    {
        uint32_t i = 0;
        for(i = 0; i < N; i++)
        {
            gv[b][i].cis = &(ptr->stripe_list[stripe_id[b]][i]);
            gv[b][i].ok = 0;
        }

        int ok = 0, t = 0;

        for(i = 0, t = 0; t < K; i++)
        {
            //the fault one
            if(gv[b][i].cis->ring_id == ring_id[b])
                continue;

            get_value_from_chunk_batch(ptr, pos_len_list[b], count[b], &(gv[b][i]), gb, &num);

            printf("[Data]gather,batch=%u,b=%u,i=%u,t=%u,num=%u\n", batch, b, i, t, num);
            t++;
        }
        if(ok >= K)
            err[b] = 0;
        else
            err[b] = 1;

        printf("batch\n");
    }

    get_batch(ptr, gb, num);

    return err;

}


static void get_value_from_chunk_batch_parity(struct ECHash_st *ptr, struct gather_value *gv, struct gather_batch *gb, uint32_t *num)
{
    gv->value = (char *)calloc(1, CHUNK_SIZE * sizeof(char));
    uint32_t ring_id = gv->cis->ring_id;

    if(gv->cis->key)
    {
        //stripe:%u-parity:%u
        struct memcached_st *tmp = ptr->rings[ring_id].ring;
        uint32_t index = memcached_generate_hash_with_redistribution(tmp, gv->cis->key, strlen(gv->cis->key));

        gb[*num].ring_id = ring_id;
        gb[*num].index = index;
        strcpy(gb[*num].key, gv->cis->key);
        gb[*num].pos = gv->value;


        printf("[Parity]Num=%u,ring_id=%u,index=%u,key[%s],pos=%p\n", *num, gb[*num].ring_id, gb[*num].index, gb[*num].key, gb[*num].pos);
        (*num)++;
        all_recovery_kv_parity++;
        frag_recovery_kv_parity++;
        gv->ok = 1;

    }
    else//data
    {
        uint32_t chunk_id = gv->cis->chunk_id;
        struct key_st *p = ptr->chunk_list[chunk_id].key_list;
        char *key;

        while(p)
        {
            if(p->hn)
            {
                key = p->hn->key;

                //stripe:%u-parity:%u
                uint32_t pos = GET_POSITION(p->hn->value);


                all_for_parity_kv++;

                all_recovery_kv_data++;
                if(1)
                {
                    struct memcached_st *tmp = ptr->rings[ring_id].ring;
                    uint32_t index = memcached_generate_hash_with_redistribution(tmp, key, strlen(key));

                    gb[*num].ring_id = ring_id;
                    gb[*num].index = index;
                    strcpy(gb[*num].key, key);
                    gb[*num].pos = gv->value + pos;

                    printf("[Parity]Num=%u,ring_id=%u,index=%u,key[%s],pos=%p\n", *num, gb[*num].ring_id, gb[*num].index, gb[*num].key, gb[*num].pos);

                    (*num)++;
                    frag_for_parity_kv++;
                    frag_recovery_kv_data++;
                }
            }
            p = p->next;
        }
        gv->ok = 1;
    }
}


int *gather_other_value_batch_parity(int batch, struct ECHash_st *ptr, uint32_t *ring_id, uint32_t *stripe_id, struct gather_value (*gv)[N])
{

    int *err = (int *)malloc(batch * sizeof(int));
    uint32_t num = 0;
    struct gather_batch gb[BUFFER_PARITY] = {0};

    for(uint32_t i = 0; i < BUFFER_PARITY; i++)
    {
        gb[i].key[0] = '\0';
    }

    for(int b = 0; b < batch; b++)
    {
        uint32_t i = 0;
        for(i = 0; i < N; i++)
        {
            gv[b][i].cis = &(ptr->stripe_list[stripe_id[b]][i]);
            gv[b][i].ok = 0;
        }


        int ok = 0, t = 0;

        for(i = 0, t = 0; t < K; i++)
        {
            if(gv[b][i].cis->ring_id == ring_id[b])
                continue;

            get_value_from_chunk_batch_parity(ptr, &(gv[b][i]), gb, &num);

            //num=num+n;
            printf("[Parity]gather,batch=%u,b=%u,i=%u,t=%u,num=%u\n", batch, b, i, t, num);
            t++;
        }
        if(ok >= K)
            err[b] = 0;
        else
            err[b] = 1;

        printf("batch\n");
    }
    get_batch(ptr, gb, num);
    return err;
}