#pragma once

/*value:	0-11	length
			12-23	position
			24-55	chunk_id
			56-63	key_length
*/

#define GET_KEY_LENGTH(value) ((uint32_t)(value>>56))
#define GET_CHUNK_ID(value) ((uint32_t)(value>>24) & 0xffffffff)
#define GET_POSITION(value) ((uint32_t)(value>>12) & 0xfff)
#define GET_LENGTH(value) ((uint32_t)(value & 0xfff))

uint64_t create_value(uint32_t key_length, uint32_t chunk_id, uint32_t position, uint32_t length);

void hash_table_init(struct hash_node *hash_table[]);

void hash_table_destory(struct hash_node *hash_table[]);

struct hash_node *hash_node_init(const char *key, const uint64_t value);

char *display(struct hash_node *hn);

uint32_t hash_coding(const char *key);

struct hash_node *find_hash_table(struct hash_node *hash_table[], const char *key, uint32_t hv);

uint64_t get_value_hash_table(struct hash_node *hash_table[], const char *key);

void insert_hash_table_hv(struct hash_node *hash_table[], struct hash_node *hn, uint32_t hv);
void insert_hash_table_key(struct hash_node *hash_table[], struct hash_node *hn, const char *key);

int del_hash_table_key(struct hash_node *hash_table[], const char *key);