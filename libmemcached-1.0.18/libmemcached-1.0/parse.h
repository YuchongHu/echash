/* LibMemcached
 * Copyright (C) 2010 Brian Aker
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 * Summary: Work with fetching results
 *
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

LIBMEMCACHED_API
memcached_server_list_st memcached_servers_parse(const char *server_strings);

#ifdef __cplusplus
}
#endif
