#include "libmemcached/common.h"
#include "../libhashkit/common.h"

uint64_t create_value(uint32_t key_length, uint32_t chunk_id, uint32_t position, uint32_t length)
{
    uint64_t tmp = 0;
    tmp = tmp | length;
    tmp = tmp | ((uint64_t)position << 12);
    tmp = tmp | ((uint64_t)chunk_id << 24);
    tmp = tmp | ((uint64_t)key_length << 56);
    return tmp;
}

void hash_table_init(struct hash_node *hash_table[])
{
    memset(hash_table, 0, sizeof(struct hash_node *)*HASH_MAX_SIZE);
}

void hash_table_destory(struct hash_node *hash_table[])
{
    int i = 0;
    for(i = 0; i < HASH_MAX_SIZE; i++)
    {
        struct hash_node *p = hash_table[i], *pre = p;
        while(p)
        {
            pre = p;
            p = p->next;
            free(pre);
        }
    }
}

struct hash_node *hash_node_init(const char *key, const uint64_t value)
{
    struct hash_node *hn = NULL;
    hn = (struct hash_node *)malloc(sizeof(struct hash_node));
    hn->key = (char *)malloc(sizeof(char) * strlen(key) + 1);
    strcpy(hn->key, key);
    hn->key[strlen(key)] = '\0';
    hn->value = value;
    hn->next = NULL;
    return hn;
}

char *display(struct hash_node *hn)
{
    char *s = (char *)malloc(10000 * sizeof(char));
    int n = sprintf(s, "%c%s%s%llu%c", '[', hn->key, "]:[", hn->value, ']');

    s[n] = '\0';
    return s;
}

uint32_t hash_coding(const char *key)
{
    uint32_t hv = hashkit_jenkins(key, strlen(key), 0);
    return hv;
}

struct hash_node *find_hash_table(struct hash_node *hash_table[], const char *key, uint32_t hv)
{
    if(key == NULL)
        return NULL;
    struct hash_node *hn = NULL;
    //uint32_t hv=Get_HashCode(key);
    hn = hash_table[hv % HASH_MAX_SIZE];
    struct hash_node *ret = NULL;
    while(hn)
    {
        if(strcmp(key, hn->key) == 0)
        {
            ret = hn;
            break;
        }
        hn = hn->next;
    }
    return ret;
}

uint64_t get_value_hash_table(struct hash_node *hash_table[], const char *key)
{
    if(key == NULL)
        return 0;
    uint64_t ret_value = 0;
    uint32_t hv = hashkit_jenkins(key, strlen(key), 0);

    struct hash_node *hn = hash_table[hv % HASH_MAX_SIZE];
    while(hn)
    {
        if(strcmp(key, hn->key) == 0)
        {
            ret_value = hn->value;
            break;
        }
        hn = hn->next;
    }
    if(hn == NULL)
        return 0;
    return ret_value;
}

void insert_hash_table_hv(struct hash_node *hash_table[], struct hash_node *hn, uint32_t hv)
{
    if(hn == NULL)
        return ;
    hn->next = hash_table[hv % HASH_MAX_SIZE];
    hash_table[hv % HASH_MAX_SIZE] = hn;
}

void insert_hash_table_key(struct hash_node *hash_table[], struct hash_node *hn, const char *key)
{
    uint32_t hv = hashkit_jenkins(key, strlen(key), 0);
    if(hn == NULL)
        return ;
    hn->next = hash_table[hv % HASH_MAX_SIZE];
    hash_table[hv % HASH_MAX_SIZE] = hn;
}

int del_hash_table_key(struct hash_node *hash_table[], const char *key)
{
    if(key == NULL)
        return 1;
    struct hash_node *hn = NULL;
    uint32_t hv = hashkit_jenkins(key, strlen(key), 0);

    hn = hash_table[hv % HASH_MAX_SIZE];
    if(hn && (strcmp(key, hn->key) == 0))
    {
        hash_table[hv % HASH_MAX_SIZE] = hn->next;
    }
    else
    {
        struct hash_node *pre = hn;
        while(hn)
        {
            if(strcmp(key, hn->key) == 0)
                break;
            pre = hn;
            hn = hn->next;
        }
        if(hn == NULL)
            return 1;
        pre->next = hn->next;
        free(hn);
    }
    return 0;
}