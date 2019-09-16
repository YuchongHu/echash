#include "libmemcached/common.h"

void stripe_list_expand(struct ECHash_st *ptr)
{
    ptr->stripe_list_size = ptr->stripe_list_size * 2;
    ptr->stripe_list = (struct chunk_info_st (*)[RING_SIZE])realloc(ptr->stripe_list, ptr->stripe_list_size * sizeof(struct chunk_info_st) * RING_SIZE);
    if(ptr->stripe_list == NULL)
    {
        printf("\nMemory is out at chunk_list appending.\n");
        exit(-1);
    }
    printf("\nExpand stripe_list from %u to %u\n", ptr->stripe_list_size / 2, ptr->stripe_list_size);
}

void stripe_list_set(struct ECHash_st *ptr, struct chunk_info_st *tmp_cis, uint32_t stripe_id)
{
    //double
    if(stripe_id >= ptr->stripe_list_size)
        stripe_list_expand(ptr);

    uint32_t i = 0;
    for(i = 0; i < K; i++) //front K data, in order
    {
        ptr->stripe_list[stripe_id][i].ring_id = tmp_cis[i].ring_id;
        ptr->stripe_list[stripe_id][i].chunk_id = tmp_cis[i].chunk_id;
    }
    for(i = K; i < N; i++)
    {
        ptr->stripe_list[stripe_id][i].ring_id = tmp_cis[i].ring_id;
        ptr->stripe_list[stripe_id][i].key = tmp_cis[i].key;
    }
}



