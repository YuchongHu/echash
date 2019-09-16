#pragma once

#include <libmemcached-1.0/struct/ring.h>

#ifdef __cplusplus
extern "C" {
#endif

LIBMEMCACHED_API
memcached_return_t ECHash_init(struct ECHash_st **ptr);

LIBMEMCACHED_API
memcached_return_t ECHash_destroy(struct ECHash_st *ptr);

LIBMEMCACHED_API
memcached_return_t ECHash_init_addserver(struct ECHash_st *ptr,
        const char *hostname, in_port_t port, uint32_t ring_id);
LIBMEMCACHED_API
memcached_return_t ECHash_init_addserver_by_st(struct ECHash_st *ptr, struct server_add_st *st);

LIBMEMCACHED_API
memcached_return_t ECHash_init_addserver_by_list(struct ECHash_st *ptr, struct server_add_st *st[], int add_num);

LIBMEMCACHED_API
memcached_return_t ECHash_addserver(struct ECHash_st *ptr,
        const char *hostname, in_port_t port, uint32_t ring_id);

LIBMEMCACHED_API
memcached_return_t ECHash_addserver_by_st(struct ECHash_st *ptr, struct server_add_st *st);

LIBMEMCACHED_API
memcached_return_t ECHash_addserver_by_list(struct ECHash_st *ptr, struct server_add_st *st[], int add_num);

//remove server
LIBMEMCACHED_API
memcached_return_t ECHash_remove_server(struct ECHash_st *ptr,
        const char *hostname, in_port_t port, uint32_t ring_id);

//repair server
LIBMEMCACHED_API
memcached_return_t ECHash_repair_simulation(struct ECHash_st *ptr, const char *hostname, in_port_t port, uint32_t ring_id);


LIBMEMCACHED_API
uint32_t ECHash_stat_waitting_chunk(struct ECHash_st *ptr, uint32_t ring_id);

LIBMEMCACHED_API
void ECHash_stat(struct ECHash_st *ptr, uint32_t stat[], double stat2[]);

LIBMEMCACHED_API
void ECHash_backup(struct ECHash_st *ptr);

LIBMEMCACHED_API
memcached_return_t ECHash_set(struct ECHash_st *ptr, const char *key, size_t key_length,
                                   const char *value, size_t value_length,
                                   time_t expiration,
                                   uint32_t  flags);

LIBMEMCACHED_API //dget_flag=0 ==>dget, =1, do not dget
char *ECHash_get(struct ECHash_st *ptr, const char *key, size_t key_length,
                      size_t *value_length,
                      uint32_t  *flags,
                      memcached_return_t *error);

// del item to servers
// LIBMEMCACHED_API
// memcached_return_t ECHash_del(struct ECHash_st *ptr,const char *key, size_t key_length,
//                                         time_t expiration);

#ifdef __cplusplus
}
#endif