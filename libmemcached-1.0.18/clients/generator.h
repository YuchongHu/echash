/* LibMemcached
 * Copyright (C) 2006-2009 Brian Aker
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 * Summary:
 *
 */

/*
  Code to generate data to be pushed into memcached
*/

#pragma once

typedef struct pairs_st pairs_st;

struct pairs_st {
  char *key;
  size_t key_length;
  char *value;
  size_t value_length;
};

#ifdef __cplusplus
extern "C" {
#endif

pairs_st *pairs_generate(uint64_t number_of, size_t value_length);
void pairs_free(pairs_st *pairs);

#ifdef __cplusplus
} // extern "C"
#endif
