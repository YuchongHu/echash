#pragma once

struct parity_kv
{
    uint32_t stripe_id;
    uint32_t ring_id;
    uint32_t key_length;
    char *key;
    char *value;
};

char *transfer_ustr_to_str(unsigned char *s, uint32_t len);

unsigned char *transfer_str_to_ustr(char *s, uint32_t len);

void clear_encode_st(struct ECHash_st *pt);

void destroy_encode_st(struct ECHash_st *ptr);

int try_encode(struct ECHash_st *ptr, struct parity_kv kv[N - K], struct chunk_info_st tmp[N]);

void encode(struct ECHash_st *ptr, uint32_t encode_data_ring[K], struct parity_kv kv[N - K], struct chunk_info_st tmp[N]);

char *decode(struct ECHash_st *ptr, struct gather_value gv[N], uint32_t chunk_id);

char *decode_parity(struct ECHash_st *ptr, struct gather_value gv[N], uint32_t ring_id);