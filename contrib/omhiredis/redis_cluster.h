#ifndef POCO_REDIS_CLUSTER_H
#define POCO_REDIS_CLUSTER_H

#include <stdint.h>

#include <stdarg.h>
#include "hiredis/hiredis.h"

uint16_t _crc16(const char *buf, int len);

/* redisContext link list */
typedef struct {
    redisContext *ctx;
    char ip[64];
    int port;
    int id;
} redis_cluster_node_st;
redis_cluster_node_st *_redis_cluster_node_init(int id, const char *ip, int port);
void _redis_cluster_node_free(redis_cluster_node_st *cluster_node);

/* Pipelining cache */
typedef struct {
    int slot;
    char fmt[256 + 1];
    va_list ap;
    int valid_ap;
} _append_slot_record;

#define DEFAULT_LIST_SIZE 4096
typedef struct {
    _append_slot_record *list;
    int list_size;
    int count;
    int pos;
} _append_slot_list;
_append_slot_list *_slot_list_init();
void _slot_list_free(_append_slot_list *slot_list);
void _slot_list_reset(_append_slot_list *slot_list);
int _slot_list_add(_append_slot_list *slot_list, int slot, const char *fmt, va_list ap);
_append_slot_record *_slot_list_get(_append_slot_list *slot_list);

/* Cluster manager */
#define REDIS_CLUSTER_NODE_COUNT 256
#define REDIS_CLUSTER_SLOTS 16384
typedef struct {
    int node_count;
    redis_cluster_node_st *redis_nodes[REDIS_CLUSTER_NODE_COUNT];
    redis_cluster_node_st *slots_handler[REDIS_CLUSTER_SLOTS];
    int state;
    struct timeval timeout;
    _append_slot_list *slot_list;

    uint32_t host_mask_;
    uint32_t host_dest_;
	const char* errstr;
} redis_cluster_st;
int _redis_cluster_refresh(redis_cluster_st *cluster);
int _redis_cluster_refresh_from_reply(redis_cluster_st *cluster, const redisReply *reply);
void _redis_cluster_set_slot(redis_cluster_st *cluster, redis_cluster_node_st *cluster_node, int slot);
int _redis_cluster_find_connection(redis_cluster_st *cluster, const char *ip, int port);

/* Inner interface */
int _redis_command_ping(redisContext *ctx);
redisReply *_redis_command_cluster_slots(redisContext *ctx);

void _redis_cluster_hostmask_exchang(uint32_t mask, uint32_t dest, char *host);

/* Client interface */
redis_cluster_st *redis_cluster_init();
int redis_cluster_connect(redis_cluster_st *cluster, const char (*ips)[64], int *ports, int count, int timeout);
void redis_cluster_free(redis_cluster_st *cluster);

int redis_cluster_set_hostmask(redis_cluster_st *cluster, uint32_t mask, uint32_t dest);

redisReply *redis_cluster_execute(redis_cluster_st *cluster, const char *key, const char *fmt, ...);
redisReply *redis_cluster_v_execute(redis_cluster_st *cluster, const char *key, const char *fmt, va_list ap);
redisReply *redis_cluster_arg_execute(redis_cluster_st *cluster, int slot, const char *fmt, va_list ap);
int redis_cluster_append(redis_cluster_st *cluster, const char *key, const char *fmt, ...);
int redis_cluster_v_append(redis_cluster_st *cluster, const char *key, const char *fmt, va_list ap);
int redis_cluster_arg_append(redis_cluster_st *cluster, int slot, const char *fmt, va_list ap);
redisReply *redis_cluster_get_reply(redis_cluster_st *cluster);

#endif // POCO_REDIS_CLUSTER_H
