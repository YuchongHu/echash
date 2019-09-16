#include "libmemcached/common.h"

__thread uint32_t CHUNK_ID_INC = 0; //global chunk_id;

void chunk_waitting_init(struct ECHash_st *ptr, uint32_t ring_id)
{
    ptr->rings[ring_id].waitting_length = 0;
    ptr->rings[ring_id].sealing_chunk_num = 0;
    ptr->rings[ring_id].chunk_waitting_list = 0;
}

void chunk_waitting_destroy(struct ECHash_st *ptr, uint32_t ring_id)
{
    struct chunk_waitting_st *p = ptr->rings[ring_id].chunk_waitting_list, *q = p;
    while(p)
    {
        q = p->next;
        free(p->head);
        free(p);

        p = q;
    }
    ptr->rings[ring_id].waitting_length = 0;
    ptr->rings[ring_id].sealing_chunk_num = 0;
}

struct chunk_waitting_st *chunk_waitting_push(struct ECHash_st *ptr, uint32_t ring_id, uint32_t index_tag)
{
    struct chunk_waitting_st *cws = (struct chunk_waitting_st *)malloc(sizeof(struct chunk_waitting_st));
    cws->ring_id = ring_id;

    //default=-1
    cws->index_tag = index_tag;

    cws->chunk_id = CHUNK_ID_INC++;
    cws->KV_num = 0;

    cws->head = (char *)calloc(1, CHUNK_SIZE * sizeof(char));
    cws->current = cws->head;

    cws->chunk_used_size = 0;
    cws->can_sealing = 0;

    struct chunk_waitting_st *p = ptr->rings[ring_id].chunk_waitting_list;

    //tail insert
    if(p)
    {
        while(p->next)
        {
            p = p->next;
        }
        p->next = cws;
    }
    else//list is empty
    {
        ptr->rings[ring_id].chunk_waitting_list = cws;
    }
    cws->next = 0;

    ptr->rings[ring_id].waitting_length++;

    printf("Push chunk_id=%u into ring=%u chunk_waitting_list\n", cws->chunk_id, ring_id);

    return cws;
}


struct chunk_waitting_st *chunk_waitting_pop(struct ECHash_st *ptr, uint32_t ring_id)
{
    struct chunk_waitting_st *p = ptr->rings[ring_id].chunk_waitting_list, *q = p;
    //p is the first one,can sealing
    if(p->can_sealing == 1)
    {
        ptr->rings[ring_id].chunk_waitting_list = p->next;
    }
    else
    {
        while(p->can_sealing == 0)
        {
            q = p;
            p = p->next;
        }
        //p !=NULL;
        q->next = p->next;
    }

    ptr->rings[ring_id].waitting_length--;
    ptr->rings[ring_id].sealing_chunk_num--;
    return p;
}

uint32_t chunk_waitting_set_kv(const char *key, uint32_t *chunk_id, struct ECHash_st *ptr, uint32_t ring_id, uint32_t index_tag, const char *value, size_t value_length)
{
    struct chunk_waitting_st *p = ptr->rings[ring_id].chunk_waitting_list;

    //for same server
    // while(p)
    // {
    //     if(p->index_tag == index_tag && CHUNK_SIZE - p->chunk_used_size >= value_length)
    //         break;
    //     p = p->next;
    // }

    //for normal
    while(p)
    {
        if(CHUNK_SIZE - p->chunk_used_size >= value_length)
            break;
        p = p->next;
    }

    if(p)
    {
        printf("[OLD]");
        memcpy(p->current, value, value_length);

        uint32_t pos = abs(p->current - p->head);
        p->current = p->current + value_length;
        p->chunk_used_size = p->chunk_used_size + value_length;
        p->KV_num++;

        printf("Ring=%u,key[%s],index_tag=%u,size=%u,chunk=%u,waitting=%u,sealing=%u,KV=%u,pos=%u,len=%zu, used=%u\n", \
               ring_id, key, p->index_tag, ptr->rings[ring_id].value_size, p->chunk_id, ptr->rings[ring_id].waitting_length, ptr->rings[ring_id].sealing_chunk_num, p->KV_num, pos, value_length, p->chunk_used_size);
        if(1.0 * p->chunk_used_size / CHUNK_SIZE >= CHUNK_SEALED_FACTOR)
        {
            if(p->can_sealing == 0)
            {
                p->can_sealing = 1;
                ptr->rings[ring_id].sealing_chunk_num++;
            }
            printf("Ring=%u,index_tag=%u,size=%u,chunk=%u can sealing with factor=%f,now waitting=%u,sealing=%u\n", \
                   ring_id, p->index_tag, ptr->rings[ring_id].value_size, p->chunk_id, 1.0 * p->chunk_used_size / CHUNK_SIZE, ptr->rings[ring_id].waitting_length, ptr->rings[ring_id].sealing_chunk_num);
        }

        *chunk_id = p->chunk_id;
        return pos;
    }
    else//push a new chunk
    {
        struct chunk_waitting_st *q = chunk_waitting_push(ptr, ring_id, index_tag);
        printf("[NEW]");
        memcpy(q->current, value, value_length);
        uint32_t pos = q->current - q->head;

        q->current = q->current + value_length;
        q->chunk_used_size = q->chunk_used_size + value_length;
        q->KV_num++;

        printf("Ring=%u,key[%s],index_tag=%u,size=%u,chunk=%u,waitting=%u,sealing=%u,KV=%u,pos=%u,len=%zu, used=%u\n", \
               ring_id, key, q->index_tag, ptr->rings[ring_id].value_size, q->chunk_id, ptr->rings[ring_id].waitting_length, ptr->rings[ring_id].sealing_chunk_num, q->KV_num, pos, value_length, q->chunk_used_size);

        if(1.0 * q->chunk_used_size / CHUNK_SIZE >= CHUNK_SEALED_FACTOR)
        {
            if(q->can_sealing == 0)
            {
                q->can_sealing = 1;
                ptr->rings[ring_id].sealing_chunk_num++;
            }

            printf("Ring=%u,index_tag=%u,size=%u,chunk=%u can sealing with factor=%f,now waitting=%u,sealing=%u\n", \
                   ring_id, q->index_tag, ptr->rings[ring_id].value_size, q->chunk_id, 1.0 * q->chunk_used_size / CHUNK_SIZE, ptr->rings[ring_id].waitting_length, ptr->rings[ring_id].sealing_chunk_num);

        }

        *chunk_id = q->chunk_id;

        return pos;
    }

}

void chunk_list_expand(struct ECHash_st *ptr)
{
    ptr->chunk_list_size = ptr->chunk_list_size * 2;
    ptr->chunk_list = (struct chunk_st *)realloc(ptr->chunk_list, ptr->chunk_list_size * sizeof(struct chunk_st));
    if(ptr->chunk_list == NULL)
    {
        printf("\nMemory is out at chunk_list appending.\n");
        exit(-1);
    }
    printf("\n Expand chunk_list from %u to %u\n", ptr->chunk_list_size / 2, ptr->chunk_list_size);
}

void chunk_list_append_key(struct ECHash_st *ptr, uint32_t chunk_id, struct hash_node *hn)
{
    struct key_st *ks = (struct key_st *)malloc(sizeof(struct key_st));
    ks->hn = hn;
    ks->next = 0;

    //double
    if(chunk_id >= ptr->chunk_list_size)
        chunk_list_expand(ptr);

    struct key_st *p = ptr->chunk_list[chunk_id].key_list;
    if(p)
    {
        while(p->next)
        {
            p = p->next;
        }
        p->next = ks;
    }
    else
    {
        ptr->chunk_list[chunk_id].key_list = ks;
    }
}

void chunk_list_set(struct ECHash_st *ptr, struct chunk_waitting_st *cws, uint32_t stripe_id)
{
    uint32_t chunk_id = cws->chunk_id;
    ptr->chunk_list[chunk_id].stat = Sealed;
    ptr->chunk_list[chunk_id].stripe_id = stripe_id;
    ptr->chunk_list[chunk_id].used_size = cws->chunk_used_size;
    ptr->chunk_list[chunk_id].KV_num = cws->KV_num;

    printf("Pop ring_id=%u,chunk_id=%u,stripe_id=%u,KV_num=%u\n", cws->ring_id, chunk_id, stripe_id, cws->KV_num);

    // struct key_st *p = ptr->chunk_list[chunk_id].key_list;

    // while(p)
    // {
    //     if(p->hn)
    //     {
    //         printf("key[%s]-->", p->hn->key);
    //     }
    //     else
    //     {
    //         printf("key[ NULL ]-->");
    //     }

    //     p = p->next;
    // }

    // printf("NULL\n");
    free(cws->head);
    free(cws);
}