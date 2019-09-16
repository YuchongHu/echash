#pragma once

//RS(N,K), K->data chunks, N->data+parity chunks
#define N 5
#define RING_SIZE 5

#define K 3

#define CHUNK_SIZE (4ll<<10)//4KB size
#define CHUNK_SEALED_FACTOR 0.95 //(0,1)
/*To do*/
#define CHUNK_DELETE_FACTOR 0.75 //if the chunk_size < factor,delete

#define CHUNK_LIST_INIT 10000
#define STRIPE_LIST_INIT 1000

#define HASH_MAX_SIZE 1000003
#define RING_VIRTUAL  1000

//Batch fetch for repair
#define BATCH_DATA 50
#define BATCH_PARITY 10
#define BUFFER_DATA 4096
#define BUFFER_PARITY 2048

enum chunk_stat { Waitting, Sealed, Sealed_repair, Abandon };

struct hash_node
{
    char *key;
    uint64_t  value;

    struct hash_node *next;
};

struct server_add_st
{
    char *hostname;
    in_port_t port;
    uint32_t ring_id;
};

/*To do*/
struct balance_st
{
    uint32_t index;
    uint32_t value;
};

struct chunk_waitting_st
{
    uint32_t ring_id;
    uint32_t index_tag; //Mark the index which pops the sealed chunk
    uint32_t chunk_id; //system global variable when waitting_chunk was malloc.
    int KV_num;
    char *head;
    char *current;
    uint32_t chunk_used_size;
    int can_sealing; //if chunk_used_size/4K > CHUNK_SEALED_FACTOR, can_sealing=1
    struct chunk_waitting_st *next;
};

//For migrating and writting during the miagartion
struct kv_set_waitting_st
{
    size_t key_length;
    char *key;
    size_t value_length;
    char *value;

    uint32_t  flags;
    struct kv_set_waitting_st *next;
};

struct memcached_ring_st
{
    memcached_st *ring;
    struct hash_node *hash_table[HASH_MAX_SIZE];
    struct kv_set_waitting_st *kv_set_waitting_list;
    uint32_t value_size;
    uint32_t waitting_length;
    uint32_t sealing_chunk_num;
    struct chunk_waitting_st *chunk_waitting_list;
};

struct key_st
{
    struct hash_node *hn;
    struct key_st *next;
};

//Transfer from chunk_waitting_st
struct chunk_st
{
    enum chunk_stat stat;
    uint32_t stripe_id;
    uint32_t used_size;
    uint32_t KV_num;
    struct key_st *key_list;

};

struct chunk_info_st
{
    uint32_t ring_id;
    uint32_t chunk_id;
    char *key;
};

//calloc when it inits, then memset each time
struct encode_st
{
    unsigned char *source_data[N];
    unsigned char encode_gftbl[32 * K * (N - K)];
    unsigned char encode_matrix[N * K];
    unsigned char *left_data[K];
    unsigned char error_matrix[N * K];
    unsigned char invert_matrix[N * K];
    unsigned char decode_matrix[N * K];
    unsigned char decode_gftbl[32 * K * (N - K)];
    unsigned char *recovery_data[N - K];
};

struct ECHash_st
{
    uint32_t total_num_server;
    struct memcached_ring_st rings[RING_SIZE];
    uint32_t chunk_list_size;
    struct chunk_st *chunk_list;
    uint32_t stripe_list_size;
    struct chunk_info_st (*stripe_list)[RING_SIZE];

    struct encode_st encode;
    struct balance_st *balance_arr;
};