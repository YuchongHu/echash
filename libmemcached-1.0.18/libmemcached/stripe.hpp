#pragma once

extern __thread uint32_t STRIPE_ID_INC;//global stripe_id;

void stripe_list_expand(struct ECHash_st *ptr);

void stripe_list_set(struct ECHash_st *ptr, struct chunk_info_st *tmp_cis, uint32_t stripe_id);