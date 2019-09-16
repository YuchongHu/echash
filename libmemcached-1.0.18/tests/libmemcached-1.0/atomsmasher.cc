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
  Sample test application.
*/
#include <mem_config.h>

#include <libtest/test.hpp>

#include <libmemcached-1.0/memcached.h>

#include <cstdio>
#include <cstdlib>
#include <stdint.h>
#include <cstring>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <clients/generator.h>
#include <clients/execute.h>

#include <libtest/server.h>

#include <tests/debug.h>

#include "tests/libmemcached-1.0/generate.h"

using namespace libtest;

/* Number of items generated for tests */
#define GLOBAL_COUNT 100000

/* Number of times to run the test loop */
#define TEST_COUNTER 500000

static pairs_st *global_pairs;
static char *global_keys[GLOBAL_COUNT];
static size_t global_keys_length[GLOBAL_COUNT];

static test_return_t cleanup_pairs_TEST(memcached_st *)
{
  pairs_free(global_pairs);

  return TEST_SUCCESS;
}

static test_return_t generate_pairs_TEST(memcached_st *)
{
  global_pairs= pairs_generate(GLOBAL_COUNT, 400);

  for (ptrdiff_t x= 0; x < GLOBAL_COUNT; x++)
  {
    global_keys[x]= global_pairs[x].key;
    global_keys_length[x]=  global_pairs[x].key_length;
  }

  return TEST_SUCCESS;
}

static test_return_t drizzle_TEST(memcached_st *memc)
{
infinite:
  for (ptrdiff_t x= 0; x < TEST_COUNTER; x++)
  {
    memcached_return_t rc;

    uint32_t test_bit= (uint32_t)(random() % GLOBAL_COUNT);
    uint8_t which= (uint8_t)(random() % 2);

    if (which == 0)
    {
      size_t return_value_length;
      uint32_t flags;
      char* return_value= memcached_get(memc, global_keys[test_bit], global_keys_length[test_bit],
                                        &return_value_length, &flags, &rc);
      if (rc == MEMCACHED_SUCCESS && return_value)
      {
        free(return_value);
      }
      else if (rc == MEMCACHED_NOTFOUND)
      {
        continue;
      }
      else
      {
        test_compare(MEMCACHED_SUCCESS, rc);
      }
    }
    else
    {
      rc= memcached_set(memc, global_pairs[test_bit].key,
                        global_pairs[test_bit].key_length,
                        global_pairs[test_bit].value,
                        global_pairs[test_bit].value_length,
                        0, 0);
      if (rc != MEMCACHED_SUCCESS && rc != MEMCACHED_BUFFERED)
      {
        test_compare(MEMCACHED_SUCCESS, rc);
      }
    }
  }

  if (getenv("MEMCACHED_ATOM_BURIN_IN"))
  {
    goto infinite;
  }

  return TEST_SUCCESS;
}

static test_return_t pre_nonblock(memcached_st *memc)
{
  test_skip(MEMCACHED_SUCCESS, memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 0));

  return TEST_SUCCESS;
}

/*
  Set the value, then quit to make sure it is flushed.
  Come back in and test that add fails.
*/
static test_return_t add_test(memcached_st *memc)
{
  const char *key= "foo";
  const char *value= "when we sanitize";

  memcached_return_t rc= memcached_set(memc, key, strlen(key),
                                       value, strlen(value),
                                       time_t(0), uint32_t(0));
  test_true_got(rc == MEMCACHED_SUCCESS or rc == MEMCACHED_BUFFERED, memcached_strerror(NULL, rc));
  memcached_quit(memc);
  rc= memcached_add(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);

  if (rc == MEMCACHED_CONNECTION_FAILURE)
  {
    print_servers(memc);
  }

  /* Too many broken OS'es have broken loopback in async, so we can't be sure of the result */
  if (memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_NO_BLOCK))
  {
    test_true(rc == MEMCACHED_NOTSTORED or rc == MEMCACHED_STORED);
  }
  else
  {
    test_compare_got(MEMCACHED_NOTSTORED, rc, memcached_strerror(NULL, rc));
  }

  return TEST_SUCCESS;
}

/*
 * repeating add_tests many times
 * may show a problem in timing
 */
static test_return_t many_adds(memcached_st *memc)
{
  test_true(memc);
  for (ptrdiff_t x= 0; x < TEST_COUNTER; x++)
  {
    test_compare_got(TEST_SUCCESS, add_test(memc), x);
  }
  return TEST_SUCCESS;
}

test_st smash_tests[] ={
  {"generate_pairs", true, (test_callback_fn*)generate_pairs_TEST },
  {"drizzle", true, (test_callback_fn*)drizzle_TEST },
  {"cleanup", true, (test_callback_fn*)cleanup_pairs_TEST },
  {"many_adds", true, (test_callback_fn*)many_adds },
  {0, 0, 0}
};

#define BENCHMARK_TEST_LOOP 20000

struct benchmark_state_st
{
  bool create_init;
  bool clone_init;
  memcached_st *create;
  memcached_st *clone;
} benchmark_state;

static test_return_t memcached_create_benchmark(memcached_st *)
{
  benchmark_state.create_init= true;

  for (ptrdiff_t x= 0; x < BENCHMARK_TEST_LOOP; x++)
  {
    memcached_st *ptr= memcached_create(&benchmark_state.create[x]);

    test_true(ptr);
  }

  return TEST_SUCCESS;
}

static test_return_t memcached_clone_benchmark(memcached_st *memc)
{
  benchmark_state.clone_init= true;

  for (ptrdiff_t x= 0; x < BENCHMARK_TEST_LOOP; x++)
  {
    memcached_st *ptr= memcached_clone(&benchmark_state.clone[x], memc);

    test_true(ptr);
  }

  return TEST_SUCCESS;
}

static test_return_t pre_allocate(memcached_st *)
{
  memset(&benchmark_state, 0, sizeof(benchmark_state));

  benchmark_state.create= (memcached_st *)calloc(BENCHMARK_TEST_LOOP, sizeof(memcached_st));
  test_true(benchmark_state.create);
  benchmark_state.clone= (memcached_st *)calloc(BENCHMARK_TEST_LOOP, sizeof(memcached_st));
  test_true(benchmark_state.clone);

  return TEST_SUCCESS;
}

static test_return_t post_allocate(memcached_st *)
{
  for (ptrdiff_t x= 0; x < BENCHMARK_TEST_LOOP; x++)
  {
    if (benchmark_state.create_init)
    {
      memcached_free(&benchmark_state.create[x]);
    }

    if (benchmark_state.clone_init)
    {
      memcached_free(&benchmark_state.clone[x]);
    }
  }

  free(benchmark_state.create);
  free(benchmark_state.clone);

  return TEST_SUCCESS;
}


test_st micro_tests[] ={
  {"memcached_create", 1, (test_callback_fn*)memcached_create_benchmark },
  {"memcached_clone", 1, (test_callback_fn*)memcached_clone_benchmark },
  {0, 0, 0}
};


collection_st collection[] ={
  {"smash", 0, 0, smash_tests},
  {"smash_nonblock", (test_callback_fn*)pre_nonblock, 0, smash_tests},
  {"micro-benchmark", (test_callback_fn*)pre_allocate, (test_callback_fn*)post_allocate, micro_tests},
  {0, 0, 0, 0}
};


#include "tests/libmemcached_world.h"

void get_world(libtest::Framework* world)
{
  world->collections(collection);

  world->create((test_callback_create_fn*)world_create);
  world->destroy((test_callback_destroy_fn*)world_destroy);

  world->set_runner(new LibmemcachedRunner);
}
