#include "redis_cluster.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEBUG
#ifdef DEBUG
#define _redis_cluster_log(fmt, arg...) _redis_cluster_print_log(__LINE__, fmt, ##arg)
void _redis_cluster_print_log(int line, const char *fmt, ...)
{
    char avg_buf[1024 + 1];

    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(avg_buf, 1024, fmt, args);
    va_end(args);
    avg_buf[len] = '\0';

    printf("[%d] %s\n", line, avg_buf);
}

#else
#define _redis_cluster_log(fmt, arg...)
#endif


/* crc16 algorithm*/
static const uint16_t crc16_table[256] = {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t _crc16(const char *buf, int len) {
    int i;
    uint16_t crc = 0;
    for (i = 0; i < len; i++) {
        crc = (crc << 8) ^ crc16_table[((crc >> 8) ^ *buf++) & 0x00FF];
    }
    return crc;
}

redis_cluster_node_st *_redis_cluster_node_init(int id, const char *ip, int port)
{
    redis_cluster_node_st *result = (redis_cluster_node_st *)malloc(sizeof(redis_cluster_node_st));
    if (!result) {
        return NULL;
    }

    result->ctx = NULL;
    strncpy(result->ip, ip, sizeof(result->ip));
    result->port = port;
    result->id = id;
    return result;
}

void _redis_cluster_node_free(redis_cluster_node_st *cluster_node)
{
    if (cluster_node->ctx) {
        redisFree(cluster_node->ctx);
    }
    free(cluster_node);
}

int _redis_cluster_refresh(redis_cluster_st *cluster)
{
    int rc;
    redisReply *reply;

    int i;
    for (i = 0; i < cluster->node_count; ++i) {
        if (!cluster->redis_nodes[i]->ctx) {
            cluster->redis_nodes[i]->ctx = redisConnectWithTimeout(cluster->redis_nodes[i]->ip, cluster->redis_nodes[i]->port, cluster->timeout);
            if (!cluster->redis_nodes[i]->ctx || cluster->redis_nodes[i]->ctx->err) {
                if (cluster->redis_nodes[i]->ctx) {
                    redisFree(cluster->redis_nodes[i]->ctx);
                    cluster->redis_nodes[i]->ctx = NULL;
                }
                _redis_cluster_log("Refresh init context fail.[%s:%d]", cluster->redis_nodes[i]->ip, cluster->redis_nodes[i]->port);
                continue;
            }
        }

        reply = _redis_command_cluster_slots(cluster->redis_nodes[i]->ctx);
        if (!reply || REDIS_REPLY_ARRAY != reply->type) {
            if (reply) {
                freeReplyObject(reply);
            }
            redisFree(cluster->redis_nodes[i]->ctx);
            cluster->redis_nodes[i]->ctx = NULL;
            _redis_cluster_log("Refresh get reply fail.");
            continue;
        }

        rc = _redis_cluster_refresh_from_reply(cluster, reply);
        freeReplyObject(reply);
        if (rc < 0) {
            redisFree(cluster->redis_nodes[i]->ctx);
            cluster->redis_nodes[i]->ctx = NULL;
            _redis_cluster_log("Refresh from reply fail.");
            return -1;
        }

        return 0;
    }

    return -1;
}

int _redis_cluster_refresh_from_reply(redis_cluster_st *cluster, const redisReply *reply)
{
    if (!cluster || !reply) {
        return -1;
    }
    size_t i, j;
    int k;
    int cluster_node_count = reply->elements;
    int cluster_idx = cluster_node_count;
    char ip[512];
    int port;

    for (i = 0; i < reply->elements; ++i) {
        if ( ! (reply->element[i]->elements >= 3 &&
                reply->element[i]->element[0]->type == REDIS_REPLY_INTEGER &&
                reply->element[i]->element[1]->type == REDIS_REPLY_INTEGER &&
                reply->element[i]->element[2]->type == REDIS_REPLY_ARRAY
                )
             ) {
            _redis_cluster_log("Invalid type.\n");
            return -1;
        }

        strcpy(ip, reply->element[i]->element[2]->element[0]->str);
        if (0 != cluster->host_mask_) {
            _redis_cluster_hostmask_exchang(cluster->host_mask_, cluster->host_dest_, ip);
        }
        port = reply->element[i]->element[2]->element[1]->integer;

        /* Master node */
        /* Move old master node */
        if (cluster->redis_nodes[i]) {
            _redis_cluster_node_free(cluster->redis_nodes[i]);
            cluster->redis_nodes[i] = NULL;
        }

        /* Create new master node */
        cluster->redis_nodes[i] = _redis_cluster_node_init(i, ip, port);
        if (!cluster->redis_nodes[i]) {
            _redis_cluster_log("Init new master node fail.");
            return -1;
        }

        cluster->redis_nodes[i]->ctx = redisConnectWithTimeout(cluster->redis_nodes[i]->ip, cluster->redis_nodes[i]->port, cluster->timeout);
        if (!cluster->redis_nodes[i]->ctx) {
            _redis_cluster_log("Make new connection fail.");
            return -1;
        }

        /* Slots handler */
        for (k = (int)reply->element[i]->element[0]->integer; k <= (int)reply->element[i]->element[1]->integer; ++k) {
            cluster->slots_handler[k] = cluster->redis_nodes[i];
        }

        _redis_cluster_log("Master:[%d] (%d - %d)[%s:%d]", (int)i, (int)reply->element[i]->element[0]->integer, (int)reply->element[i]->element[1]->integer, ip, port);

        /* Slave node */
        for (j = cluster_node_count; j < reply->element[i]->elements; ++j) {
            if (reply->element[i]->element[j]->type != REDIS_REPLY_ARRAY) {
                continue;
            }

            strcpy(ip, reply->element[i]->element[j]->element[0]->str);
            if (0 != cluster->host_mask_) {
                _redis_cluster_hostmask_exchang(cluster->host_mask_, cluster->host_dest_, ip);
            }
            port = reply->element[i]->element[j]->element[1]->integer;

            /* Move old slave node */
            if (cluster->redis_nodes[cluster_idx]) {
                _redis_cluster_node_free(cluster->redis_nodes[cluster_idx]);
                cluster->redis_nodes[cluster_idx] = NULL;
            }

            /* Create new slave node */
            cluster->redis_nodes[cluster_idx] = _redis_cluster_node_init(cluster_idx, ip, port);
            if (!cluster->redis_nodes[cluster_idx]) {
                _redis_cluster_log("Init new slave node fail.");
                return -1;
            }

            cluster->redis_nodes[cluster_idx]->ctx = redisConnectWithTimeout(cluster->redis_nodes[cluster_idx]->ip, cluster->redis_nodes[cluster_idx]->port, cluster->timeout);
            if (!cluster->redis_nodes[cluster_idx]->ctx) {
                _redis_cluster_log("Make new connection fail.");
                return -1;
            }

            _redis_cluster_log("Slave:[%d] [%s:%d]", cluster_idx, ip, port);

            ++cluster_idx;
        }
    }

    cluster->node_count = cluster_idx;
    return 0;
}

void _redis_cluster_set_slot(redis_cluster_st *cluster, redis_cluster_node_st *cluster_node, int slot)
{
    assert(cluster);
    assert(slot >= 0);
    cluster->slots_handler[slot] = cluster_node;
}

int _redis_cluster_find_connection(redis_cluster_st *cluster, const char *ip, int port)
{
    assert(cluster);
    assert(ip);
    assert(port >= 0);
    int i;
    for (i = 0; i < cluster->node_count; ++i) {
        if (!cluster->redis_nodes[i]) {
            continue;
        }
        if (port == cluster->redis_nodes[i]->port && 0 == strcmp(cluster->redis_nodes[i]->ip, ip)) {
            return i;
        }
    }

    return -1;
}

_append_slot_list *_slot_list_init()
{
    _append_slot_list *handler_list = (_append_slot_list *)malloc(sizeof(_append_slot_list));
    if (!handler_list) {
        return NULL;
    }

    handler_list->list = (_append_slot_record *)malloc(DEFAULT_LIST_SIZE * sizeof(_append_slot_record));
    if (!handler_list->list) {
        free(handler_list);
        return NULL;
    }

    handler_list->list_size = DEFAULT_LIST_SIZE;
    handler_list->count = 0;
    handler_list->pos = 0;
    return handler_list;
}

void _slot_list_free(_append_slot_list *slot_list)
{
    if (!slot_list) {
        return;
    }
    int i;

    if (slot_list->list) {
        for (i = 0; i < slot_list->list_size; ++i) {
            if (slot_list->list[i].valid_ap) {
                va_end(slot_list->list[i].ap);
                slot_list->list[i].valid_ap = 0;
            }
        }
        free(slot_list->list);
    }
    free(slot_list);
}

void _slot_list_reset(_append_slot_list *slot_list)
{
    slot_list->count = 0;
    slot_list->pos = 0;
}

int _slot_list_add(_append_slot_list *slot_list, int slot, const char *fmt, va_list ap)
{
    if (slot_list->count >= slot_list->list_size) {
        _append_slot_record *new_list = (_append_slot_record *)malloc(slot_list->list_size * 2 * sizeof(_append_slot_record));
        if (!new_list) {
            return - 1;
        }

        if (slot_list->list) {
            memcpy(new_list, slot_list->list, slot_list->list_size * sizeof(_append_slot_record));
            free(slot_list->list);
        }
        slot_list->list = new_list;
        slot_list->list_size *= 2;
    }

    slot_list->list[slot_list->count].slot = slot;

    if (slot_list->list[slot_list->count].valid_ap) {
        va_end(slot_list->list[slot_list->count].ap);
    }
    va_copy(slot_list->list[slot_list->count].ap, ap);
    slot_list->list[slot_list->count].valid_ap = 1;

    strncpy(slot_list->list[slot_list->count].fmt, fmt, sizeof(slot_list->list[slot_list->count].fmt) - 1);
    slot_list->list[slot_list->count].fmt[sizeof(slot_list->list[slot_list->count].fmt) - 1] = '\0';
    ++slot_list->count;

    return 0;
}

_append_slot_record *_slot_list_get(_append_slot_list *slot_list)
{
    if (!slot_list || slot_list->pos == slot_list->count) {
        return NULL;
    }

    return &slot_list->list[slot_list->pos++];
}

int _redis_command_ping(redisContext *ctx)
{
    if (!ctx) {
        return -1;
    }

    int ret = -1;
    redisReply *reply = (redisReply *)redisCommand(ctx, "PING");
    if (!reply) {
        return -1;
    }
    if (REDIS_REPLY_STRING == reply->type && 0 == strcmp(reply->str, "PONG")) {
        ret = 0;
    }
    freeReplyObject(reply);
    return ret;
}

redisReply *_redis_command_cluster_slots(redisContext *ctx)
{
    if (!ctx) {
        return NULL;
    }

    redisReply *reply = (redisReply *)redisCommand(ctx, "CLUSTER SLOTS");
    if (!reply) {
        return NULL;
    }

    return reply;
}

void _redis_cluster_hostmask_exchang(uint32_t mask, uint32_t dest, char *host)
{
    mask = htonl(mask);
    dest = htonl(dest);

    struct sockaddr_in adr_inet;
    if (!inet_aton(host, &adr_inet.sin_addr)) {
        return;
    }

    uint32_t src = adr_inet.sin_addr.s_addr;
    src &= ~mask;
    dest &= mask;
    src |= dest;
    adr_inet.sin_addr.s_addr = src;
    strcpy(host, inet_ntoa(adr_inet.sin_addr));
}

redis_cluster_st *redis_cluster_init()
{
    redis_cluster_st *cluster = (redis_cluster_st *)malloc(sizeof(redis_cluster_st));
    memset(cluster, 0x00, sizeof(redis_cluster_st));
    return cluster;
}

int redis_cluster_connect(redis_cluster_st *cluster, const char (*ips)[64], int *ports, int count, int timeout)
{
    if (!ips || !ports || count < 0 || timeout <= 0) {
        return -1;
    }
    redisContext *ctx = NULL;
    redisReply *r = NULL;
    int rc;
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = timeout % 1000;

    int i;
    for (i = 0; i < count; ++i) {
        if (!ips[i] || ports[i] <= 0) {
            continue;
        }

        ctx = redisConnectWithTimeout(ips[i], ports[i], tv);
        if (!ctx || ctx->err) {
            if (ctx) {
                redisFree(ctx);
                ctx = NULL;
            }
            _redis_cluster_log("Connect to %s:%d fail!", ips[i], ports[i]);
            continue;
        }

        r = _redis_command_cluster_slots(ctx);
        if (!r || REDIS_REPLY_ARRAY != r->type) {
            redisFree(ctx);
            if (r) {
                freeReplyObject(r);
                r = NULL;
            }
            _redis_cluster_log("Get reply fail.");
            continue;
        }
        break;
    }

    if (!r) {
        if (ctx) {
            redisFree(ctx);
            ctx = NULL;
        }
        _redis_cluster_log("Init fail.");
        return -1;
    }

    cluster->timeout = tv;
    cluster->slot_list = _slot_list_init();
    if (!cluster->slot_list) {
        _redis_cluster_log("Init slot list fail.");
        goto ON_INIT_ERROR;
    }

    rc = _redis_cluster_refresh_from_reply(cluster, r);
    if (rc < 0) {
        _redis_cluster_log("Refresh fail.");
        goto ON_INIT_ERROR;
    }

    freeReplyObject(r);
    redisFree(ctx);
    return 0;

ON_INIT_ERROR:
    if (r) {
        freeReplyObject(r);
        r = NULL;
    }
    if (ctx) {
        redisFree(ctx);
        ctx = NULL;
    }
    return -1;
}

void redis_cluster_free(redis_cluster_st *cluster)
{
    if (!cluster) {
        return;
    }
    int i;

    for (i = 0; i < cluster->node_count; ++i) {
        _redis_cluster_node_free(cluster->redis_nodes[i]);
        cluster->redis_nodes[i] = NULL;
    }
    cluster->node_count = 0;

    if (cluster->slot_list) {
        _slot_list_free(cluster->slot_list);
    }
    free(cluster);
}

int redis_cluster_set_hostmask(redis_cluster_st *cluster, uint32_t mask, uint32_t dest)
{
    cluster->host_mask_ = mask;
    cluster->host_dest_ = dest;
    return 0;
}

redisReply *redis_cluster_execute(redis_cluster_st *cluster, const char *key, const char *fmt, ...)
{
    int slot = _crc16(key, strlen(key)) % REDIS_CLUSTER_SLOTS;

    _redis_cluster_log("Key[%s] Slot[%d]", key, slot);
    va_list ap;
    va_start(ap, fmt);
    redisReply *r = redis_cluster_arg_execute(cluster, slot, fmt, ap);
    va_end(ap);

    return r;
}

redisReply *redis_cluster_v_execute(redis_cluster_st *cluster, const char *key, const char *fmt, va_list ap)
{
    int slot = _crc16(key, strlen(key)) % REDIS_CLUSTER_SLOTS;

    _redis_cluster_log("Key[%s] Slot[%d]", key, slot);
    return redis_cluster_arg_execute(cluster, slot, fmt, ap);
}

redisReply *redis_cluster_arg_execute(redis_cluster_st *cluster, int slot, const char *fmt, va_list ap)
{
    if (!cluster || slot < 0 || !fmt) {
        return NULL;
    }

    int rc;

    _slot_list_reset(cluster->slot_list);

    rc = redis_cluster_arg_append(cluster, slot, fmt, ap);
    if (rc < 0) {
        _redis_cluster_log("Append command fail in redis_cluster_arg_execute.");
        return NULL;
    }

    return redis_cluster_get_reply(cluster);
}

int redis_cluster_append(redis_cluster_st *cluster, const char *key, const char *fmt, ...)
{
    int slot = _crc16(key, strlen(key)) % REDIS_CLUSTER_SLOTS;

    _redis_cluster_log("Key[%s] Slot[%d]", key, slot);
    va_list ap;
    va_start(ap, fmt);
    int rc = redis_cluster_arg_append(cluster, slot, fmt, ap);
    va_end(ap);

    return rc;
}

int redis_cluster_v_append(redis_cluster_st *cluster, const char *key, const char *fmt, va_list ap)
{
    int slot = _crc16(key, strlen(key)) % REDIS_CLUSTER_SLOTS;
    return redis_cluster_arg_append(cluster, slot, fmt, ap);
}

int redis_cluster_arg_append(redis_cluster_st *cluster, int slot, const char *fmt, va_list ap)
{
    if (!cluster || slot < 0 || !fmt) {
        return -1;
    }

    int rc;
    int handler_idx;

    handler_idx = cluster->slots_handler[slot]->id;
    if (!cluster->redis_nodes[handler_idx]->ctx) {
        cluster->redis_nodes[handler_idx]->ctx = redisConnectWithTimeout(cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port, cluster->timeout);
        if (!cluster->redis_nodes[handler_idx]->ctx || cluster->redis_nodes[handler_idx]->ctx->err) {
            if (cluster->redis_nodes[handler_idx]->ctx) {
                redisFree(cluster->redis_nodes[handler_idx]->ctx);
                cluster->redis_nodes[handler_idx]->ctx = NULL;
            }
            _redis_cluster_log("Refresh cluster.");
            // Refresh cluster while reconnect fail.
            rc = _redis_cluster_refresh(cluster);
            if (rc < 0) {
                _redis_cluster_log("Refresh cluster fail.");
                return -1;
            }

            handler_idx = cluster->slots_handler[slot]->id;
            if (handler_idx < 0 || !cluster->redis_nodes[handler_idx]->ctx) {
                _redis_cluster_log("Find slot handler connection fail.");
                return -1;
            }
        }
        _redis_cluster_log("Reconnect success.");
    }

    _redis_cluster_log("Slot[%d] handler[%s:%d]", slot, cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port);
    assert(cluster->redis_nodes[handler_idx]->ctx);
    rc = redisvAppendCommand(cluster->redis_nodes[handler_idx]->ctx, fmt, ap);
	cluster->errstr = cluster->redis_nodes[handler_idx]->ctx->errstr;
    if (REDIS_OK != rc) {
        redisFree(cluster->redis_nodes[handler_idx]->ctx);
        cluster->redis_nodes[handler_idx]->ctx = NULL;
        return -1;
    }

    if (cluster->slot_list->pos != 0) {
        /* Next round */
        _slot_list_reset(cluster->slot_list);
    }

    rc = _slot_list_add(cluster->slot_list, slot, fmt, ap);
    if (rc < 0) {
        return -1;
    }

	cluster->errstr = NULL;
    return 0;
}

redisReply *redis_cluster_get_reply(redis_cluster_st *cluster)
{
    _append_slot_record *record = _slot_list_get(cluster->slot_list);
    if (NULL == record) {
        return NULL;
    }

    int slot = record->slot;
    int rc;
    int handler_idx;
    redisReply *reply = NULL;
    redisContext *redirect_ctx = NULL;

    char *p, *s;
    int is_ask;
    int redirect_slot;

    handler_idx = cluster->slots_handler[slot]->id;
    if (!cluster->redis_nodes[handler_idx]->ctx) {
        return NULL;
    }

    rc = redisSetTimeout(cluster->redis_nodes[handler_idx]->ctx, cluster->timeout);
    if (REDIS_OK != rc) {
        _redis_cluster_log("Set timeout fail.[%d]", rc);
        redisFree(cluster->redis_nodes[handler_idx]->ctx);
        cluster->redis_nodes[handler_idx]->ctx = NULL;
        return NULL;
    }

    rc = redisGetReply(cluster->redis_nodes[handler_idx]->ctx, (void **)&reply);
    if (REDIS_OK != rc || NULL == reply) {
        redisFree(cluster->redis_nodes[handler_idx]->ctx);
        cluster->redis_nodes[handler_idx]->ctx = NULL;
        _redis_cluster_log("Get reply fail.");
        return NULL;
    }

    /* Cluster redirection */
    is_ask = 0;
    while (REDIS_REPLY_ERROR == reply->type && (0 == strncmp(reply->str,"MOVED",5) || 0 == strncmp(reply->str,"ASK",3))) {
        if (0 == strncmp(reply->str,"ASK",3)) {
            is_ask = 1;
        }

        p = reply->str;
        /* Comments show the position of the pointer as:
         *
         * [S] for pointer 's'
         * [P] for pointer 'p'
         */
        s = strchr(p, ' ');      /* MOVED[S]3999 127.0.0.1:6381 */
        p = strchr(s + 1, ' ');    /* MOVED[S]3999[P]127.0.0.1:6381 */
        *p = '\0';
        redirect_slot = atoi(s + 1);
        s = strchr(p + 1, ':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
        *s = '\0';
        handler_idx = _redis_cluster_find_connection(cluster, p + 1, atoi(s + 1));
        freeReplyObject(reply);
        if (handler_idx < 0) {
            /* Refresh cluster nodes */
            rc = _redis_cluster_refresh(cluster);
            if (rc < 0) {
                _redis_cluster_log("Refresh cluster fail.");
                return NULL;
            }

            handler_idx = cluster->slots_handler[slot]->id;
            if (handler_idx < 0) {
                _redis_cluster_log("Find slot handler connection fail.");
                return NULL;
            }
        } else {
            if (!is_ask) {
                /* Save redirection */
                _redis_cluster_set_slot(cluster, cluster->redis_nodes[handler_idx], slot);
            }
        }

        _redis_cluster_log("Redirect slot[%d] to server[%s:%d]", redirect_slot, cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port);
        if (!cluster->redis_nodes[handler_idx]->ctx) {
            cluster->redis_nodes[handler_idx]->ctx = redisConnectWithTimeout(cluster->redis_nodes[handler_idx]->ip, cluster->redis_nodes[handler_idx]->port, cluster->timeout);
            if (!cluster->redis_nodes[handler_idx]->ctx || cluster->redis_nodes[handler_idx]->ctx->err) {
                if (cluster->redis_nodes[handler_idx]->ctx) {
                    redisFree(cluster->redis_nodes[handler_idx]->ctx);
                    cluster->redis_nodes[handler_idx]->ctx = NULL;
                }
                _redis_cluster_log("Reconnect to redis server timeout.");
                return NULL;
            }
        }

        redirect_ctx = cluster->redis_nodes[handler_idx]->ctx;
        reply = (redisReply *)(redisvCommand(redirect_ctx, record->fmt, record->ap));
        if (!reply) {
            return NULL;
        }
    }

    va_end(record->ap);
    record->valid_ap = 0;

    return reply;
}
