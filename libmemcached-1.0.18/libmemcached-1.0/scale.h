#pragma once

#include <libmemcached-1.0/struct/ring.h>


struct chunk_repair_st
{
    //For simplicty, data item <100 in 4KB chunks.
    struct hash_node *hns[100];
    uint32_t count;
    uint32_t chunk_id;

    struct chunk_repair_st *next;
};

struct pos_len_st
{
    uint32_t pos;
    uint32_t len;
};

// #ifdef __cplusplus
// extern "C" {
// #endif
// 
// LIBMEMCACHED_API
// memcached_return_t ECHash_init_addserver(struct ECHash_st *ptr,
//         const char *hostname, in_port_t port, uint32_t ring_id);
// LIBMEMCACHED_API
// memcached_return_t ECHash_init_addserver_by_st(struct ECHash_st *ptr, struct server_add_st *st);

// LIBMEMCACHED_API
// memcached_return_t ECHash_init_addserver_by_list(struct ECHash_st *ptr, struct server_add_st *st[], int add_num);

// LIBMEMCACHED_API
// memcached_return_t ECHash_addserver(struct ECHash_st *ptr,
//         const char *hostname, in_port_t port, uint32_t ring_id);

// LIBMEMCACHED_API
// memcached_return_t ECHash_addserver_by_st(struct ECHash_st *ptr, struct server_add_st *st);

// LIBMEMCACHED_API
// memcached_return_t ECHash_addserver_by_list(struct ECHash_st *ptr, struct server_add_st *st[], int add_num);

// LIBMEMCACHED_API
// memcached_return_t ECHash_remove_server(struct ECHash_st *ptr,
//         const char *hostname, in_port_t port, uint32_t ring_id);

// LIBMEMCACHED_API
// memcached_return_t ECHash_repair_simulation(struct ECHash_st *ptr, const char *hostname, in_port_t port, uint32_t ring_id);


// #ifdef __cplusplus
// }
// #endif