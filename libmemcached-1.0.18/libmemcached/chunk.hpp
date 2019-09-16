#pragma once

extern __thread uint32_t CHUNK_ID_INC;//global chunk_id;

void chunk_waitting_init(struct ECHash_st *ptr, uint32_t ring_id);

void chunk_waitting_destroy(struct ECHash_st *ptr, uint32_t ring_id);

struct chunk_waitting_st *chunk_waitting_push(struct ECHash_st *ptr, uint32_t ring_id, uint32_t index_tag);

struct chunk_waitting_st *chunk_waitting_pop(struct ECHash_st *ptr, uint32_t ring_id);

uint32_t chunk_waitting_set_kv(const char *key, uint32_t *chunk_id, struct ECHash_st *ptr, uint32_t ring_id, uint32_t index_tag, const char *value, size_t value_length);

void chunk_list_expand(struct ECHash_st *ptr);

void chunk_list_set(struct ECHash_st *ptr, struct chunk_waitting_st *cws, uint32_t stripe_id);

void chunk_list_append_key(struct ECHash_st *ptr, uint32_t chunk_id, struct hash_node *hn);