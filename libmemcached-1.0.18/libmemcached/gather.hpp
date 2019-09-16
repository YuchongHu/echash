#pragma once

#include <pthread.h>


struct gather_value
{
    struct chunk_info_st *cis;
    int ok;//1=>means the value will paticipate in decoding
    char *value;
};

struct pthread_parameter
{
    struct ECHash_st *ptr;
    struct gather_value *gv;

};

struct gather_batch
{
    uint32_t ring_id;
    uint32_t index;
    char key[250];
    char *pos;
};

void printf_arr(char *s, uint32_t len);

int gather_other_value(struct ECHash_st *ptr, uint32_t ring_id, struct pos_len_st *pos_len_list, uint32_t count, uint32_t stripe_id, struct gather_value *gv);

int *gather_other_value_batch(int batch, struct ECHash_st *ptr, uint32_t *ring_id, struct pos_len_st (*pos_len_list)[100], uint32_t *count, uint32_t *stripe_id, struct gather_value (*gv)[N]);

int *gather_other_value_batch_parity(int batch, struct ECHash_st *ptr, uint32_t *ring_id, uint32_t *stripe_id, struct gather_value (*gv)[N]);
