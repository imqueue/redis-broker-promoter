/*!
 * Extends native redis module to be promise-like
 *
 * Copyright (c) 2018, imqueue.com <support@imqueue.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 */
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <ifaddrs.h>
#include <limits.h>
#include <uuid/uuid.h>
#include "redismodule.h"

// Default values
#define DEFAULT_BROADCAST_ADDRESSES "255.255.255.255"
#define DEFAULT_BROADCAST_NAME "imq-broker"
#define DEFAULT_BROADCAST_PORT 63000
#define DEFAULT_BROADCAST_INTERVAL 1
#define MAX_REDIS_BINDS 8

static int enable_logging = 0;
static int global_redis_port = 6379;
static int global_redis_tls = 0;

static char redis_guid[37];

static char redis_bind_ips[MAX_REDIS_BINDS][INET_ADDRSTRLEN];
static int redis_bind_count = 0;
static int allow_all_interfaces = 0;

static pthread_t *thread_ids = NULL;
static int thread_count = 0;

void generate_redis_guid() {
    uuid_t binuuid;
    uuid_generate(binuuid);
    uuid_unparse_lower(binuuid, redis_guid);
}


char *get_broadcast_name() {
    char *service_name = getenv("REDIS_BROADCAST_NAME");

    if (!service_name) {
        service_name = DEFAULT_BROADCAST_NAME;
    }

    return service_name;
}

void load_redis_bind_ips(RedisModuleCtx *ctx) {
    redis_bind_count = 0;
    allow_all_interfaces = 0;

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "bind");
    if (!reply
        || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY
        || RedisModule_CallReplyLength(reply) != 2
    ) {
        if (reply) {
            RedisModule_FreeCallReply(reply);
        }

        allow_all_interfaces = 1; // fallback

        return;
    }

    RedisModuleCallReply *val_reply = RedisModule_CallReplyArrayElement(reply, 1);

    if (val_reply && RedisModule_CallReplyType(val_reply) == REDISMODULE_REPLY_STRING) {
        RedisModuleString *bind_str = RedisModule_CreateStringFromCallReply(val_reply);
        size_t len;
        const char *bind_cstr = RedisModule_StringPtrLen(bind_str, &len);
        char buf[256];

        strncpy(buf, bind_cstr, sizeof(buf));
        buf[sizeof(buf) - 1] = '\0';

        char *token = strtok(buf, " ");

        while (token && redis_bind_count < MAX_REDIS_BINDS) {
            if (strcmp(token, "0.0.0.0") == 0) {
                allow_all_interfaces = 1;

                break;
            }

            strncpy(redis_bind_ips[redis_bind_count++], token, INET_ADDRSTRLEN);
            token = strtok(NULL, " ");
        }

        RedisModule_FreeString(ctx, bind_str);
    }

    RedisModule_FreeCallReply(reply);
}

int should_broadcast_from_ip(const char *ip) {
    if (allow_all_interfaces) {
        return 1;
    }

    for (int i = 0; i < redis_bind_count; i++) {
        if (strcmp(redis_bind_ips[i], "*") == 0 || strcmp(ip, redis_bind_ips[i]) == 0) {
            return 1;
        }
    }

    return 0;
}

int parse_int(const char *buff) {
    char *end;

    errno = 0;

    const long sl = strtol(buff, &end, 10);

    if (end == buff
        || '\0' != *end
        || ((LONG_MIN == sl || LONG_MAX == sl) && ERANGE == errno)
        || sl > INT_MAX
        || sl < INT_MIN
    ) {
        return 0;
    }

    return (int)sl;
}

uint32_t infer_mask(const char *broadcast_str) {
    struct in_addr broadcast_addr;

    if (inet_aton(broadcast_str, &broadcast_addr) == 0) {
        return 0;
    }

    const uint32_t ip = ntohl(broadcast_addr.s_addr);

    if ((ip & 0x00FFFFFF) == 0x00FFFFFF) return 0xFF000000;  // /8
    if ((ip & 0x0000FFFF) == 0x0000FFFF) return 0xFFFF0000;  // /16
    if ((ip & 0x000000FF) == 0x000000FF) return 0xFFFFFF00;  // /24

    return 0;  // fallback: no match
}

// Function to get local IP address
int get_ip_for_broadcast(const char *broadcast_target, char *output_ip, const size_t output_size) {
    struct ifaddrs *ifaddr;
    struct in_addr broadcast_addr;

    if (inet_aton(broadcast_target, &broadcast_addr) == 0) {
        RedisModule_Log(
            NULL,
            "error",
            "%s: invalid broadcast target given: %s",
            get_broadcast_name(),
            strerror(errno)
        );

        return -1;
    }

    const uint32_t broadcast_ip = ntohl(broadcast_addr.s_addr);
    const uint32_t inferred_mask = infer_mask(broadcast_target);

    if (inferred_mask == 0) {
        return -1;
    }

    if (getifaddrs(&ifaddr) == -1) {
        return -1;
    }

    for (const struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        const struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
        const uint32_t iface_ip = ntohl(addr->sin_addr.s_addr);
        const uint32_t iface_broadcast = iface_ip | ~inferred_mask;

        if (iface_broadcast == broadcast_ip) {
            inet_ntop(AF_INET, &addr->sin_addr, output_ip, output_size);
            freeifaddrs(ifaddr);

            return 0;
        }
    }

    freeifaddrs(ifaddr);

    return -1;
}

int get_broadcast_port() {
    const char *env = getenv("REDIS_BROADCAST_PORT");

    if (env) {
        const int port = parse_int(env);

        if (port > 0 && port <= 65535) {
            return port;
        }
    }

    return DEFAULT_BROADCAST_PORT;  // Default port
}

int get_broadcast_interval() {
    const char *env = getenv("REDIS_BROADCAST_INTERVAL");

    if (env) {
        const int interval = parse_int(env);

        if (interval > 0) {
            return interval;
        }
    }

    return DEFAULT_BROADCAST_INTERVAL;  // Default interval
}

/*
 * Reads one integer directive out of the running configuration.
 *
 * Returns `fallback` when the directive is not there at all: `tls-port` does
 * not exist on a Redis built without TLS, and CONFIG GET answers that with an
 * empty array rather than with an error.
 */
int get_config_int(RedisModuleCtx *ctx, const char *name, const int fallback) {
    int value = fallback;
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", name);

    if (reply
        && RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY
        && RedisModule_CallReplyLength(reply) == 2
    ) {
        RedisModuleCallReply *value_reply = RedisModule_CallReplyArrayElement(reply, 1);

        if (value_reply
            && RedisModule_CallReplyType(value_reply) == REDISMODULE_REPLY_STRING
        ) {
            RedisModuleString *value_str = RedisModule_CreateStringFromCallReply(value_reply);
            size_t len;
            const char *value_cstr = RedisModule_StringPtrLen(value_str, &len);

            value = parse_int(value_cstr);

            RedisModule_FreeString(ctx, value_str);
        }
    }

    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    return value;
}

/*
 * REDIS_BROADCAST_TLS: 1 announces the TLS listener, 0 announces the plaintext
 * one, -1 (unset, or anything unrecognised) announces whichever is up.
 */
int get_tls_preference() {
    const char *env = getenv("REDIS_BROADCAST_TLS");

    if (!env || !*env) {
        return -1;
    }

    if (!strcasecmp(env, "1") || !strcasecmp(env, "yes")
        || !strcasecmp(env, "true") || !strcasecmp(env, "on")
    ) {
        return 1;
    }

    if (!strcasecmp(env, "0") || !strcasecmp(env, "no")
        || !strcasecmp(env, "false") || !strcasecmp(env, "off")
    ) {
        return 0;
    }

    RedisModule_Log(
        NULL,
        "warning",
        "%s: REDIS_BROADCAST_TLS='%s' is not one of 1/0, yes/no, true/false,"
        " on/off - ignored, the listening port decides",
        get_broadcast_name(),
        env
    );

    return -1;
}

/*
 * The port a client can actually reach this broker on, and whether reaching it
 * means TLS.
 *
 * `port 0` does not mean "no port": it is how Redis is told to stop listening
 * in plaintext, and a TLS-only broker is configured in exactly that way. This
 * announcement has always carried `port` verbatim, so such a broker announced
 * `<ip>:0` - an address nothing can connect to, and one that @imqueue's UDP
 * listener drops as malformed. The fleet then saw no broker at all, and no
 * error anywhere said why: the announcement did go out, it was just useless.
 *
 * So the announced port is the one that is listening. When BOTH are listening
 * plaintext wins, because that is what every existing deployment already
 * announces, and an image upgrade must not move a fleet onto a transport its
 * clients are not configured for. REDIS_BROADCAST_TLS=1 is how that move is
 * made deliberately.
 *
 * @param ctx - module context, used to read the configuration
 * @param is_tls - set to 1 when the returned port is the TLS listener
 * @returns the port to announce, or 0 when there is nothing worth announcing
 */
int resolve_announced_port(RedisModuleCtx *ctx, int *is_tls) {
    const int port = get_config_int(ctx, "port", 0);
    const int tls_port = get_config_int(ctx, "tls-port", 0);
    const int prefer_tls = get_tls_preference();

    *is_tls = 0;

    if (prefer_tls == 1) {
        if (tls_port > 0) {
            *is_tls = 1;

            return tls_port;
        }

        // announcing the plaintext port instead would silently undo an explicit
        // request for TLS, which is the one outcome worse than not being found
        RedisModule_Log(
            NULL,
            "warning",
            "%s: REDIS_BROADCAST_TLS asks for the TLS listener, but `tls-port`"
            " is 0 or unsupported by this build",
            get_broadcast_name()
        );

        return 0;
    }

    if (prefer_tls == 0) {
        if (port > 0) {
            return port;
        }

        RedisModule_Log(
            NULL,
            "warning",
            "%s: REDIS_BROADCAST_TLS asks for the plaintext listener, but"
            " `port` is 0",
            get_broadcast_name()
        );

        return 0;
    }

    if (port > 0) {
        return port;
    }

    if (tls_port > 0) {
        *is_tls = 1;

        return tls_port;
    }

    return 0;
}

int is_closing = 0;

typedef struct {
    char source_ip[INET_ADDRSTRLEN];
    int redis_port;
    int redis_tls;
} BroadcastTask;

// ReSharper disable once CppDFAConstantFunctionResult
void *broadcast_thread_socket(void *arg) {
    BroadcastTask *task = (BroadcastTask *)arg;
    const char *broadcast_name = get_broadcast_name();
    const int broadcast_port = get_broadcast_port();
    const int sock = socket(AF_INET, SOCK_DGRAM, 0);
    const int broadcast = 1;
    const int broadcast_interval = get_broadcast_interval();
    struct sockaddr_in bind_addr = {0};

    if (sock < 0) {
        RedisModule_Log(
            NULL,
            "error",
            "%s: socket failed: %s",
            broadcast_name,
            strerror(errno)
        );

        free(task);

        return NULL;
    }

    setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = inet_addr(task->source_ip);
    bind_addr.sin_port = 0; // let OS choose a port

    if (bind(sock, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
        RedisModule_Log(
            NULL,
            "error",
            "%s: socket bind failed: %s",
            broadcast_name,
            strerror(errno)
        );
        close(sock);
        free(task);

        return NULL;
    }

    struct sockaddr_in dest = {0};

    dest.sin_family = AF_INET;
    dest.sin_addr.s_addr = inet_addr(DEFAULT_BROADCAST_ADDRESSES);
    dest.sin_port = htons(broadcast_port);

    while (1) {
        char message[256];

        if (is_closing) {
            snprintf(
                message,
                sizeof(message),
                "%s\t%s\tdown\t%s:%d",
                broadcast_name,
                redis_guid,
                task->source_ip,
                task->redis_port
            );
        } else {
            // the transport is the SIXTH field, after the interval, and only on
            // `up`. A reader that splits on tabs and takes the first five gets
            // exactly what it got before, and `down` keeps the four fields it
            // has always had - where a fifth would land in the interval's slot
            // and be read as a timeout of NaN, which drops the whole datagram.
            snprintf(
                message,
                sizeof(message),
                "%s\t%s\tup\t%s:%d\t%d\t%s",
                broadcast_name,
                redis_guid,
                task->source_ip,
                task->redis_port,
                broadcast_interval,
                task->redis_tls ? "tls" : "plain"
            );
        }

        if (sendto(
            sock,
            message,
            strlen(message),
            0,
            (struct sockaddr *)&dest,
            sizeof(dest)
        ) < 0) {
            RedisModule_Log(
                NULL,
                "warning",
                "%s: broadcast to %s failed: %s",
                broadcast_name,
                task->source_ip,
                strerror(errno)
            );
        } else if (enable_logging) {
            RedisModule_Log(
                NULL,
                "notice",
                "%s: UDP Broadcast from %s: %s",
                broadcast_name,
                task->source_ip,
                message
            );
        }

        if (is_closing) {
            break;
        }

        sleep(broadcast_interval);
    }

    close(sock);
    free(task);

    return NULL;
}

int count_usable_interfaces() {
    struct ifaddrs *ifaddr;
    int count = 0;

    if (getifaddrs(&ifaddr) == -1) {
        return 0;
    }

    for (const struct ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        ++count;
    }

    freeifaddrs(ifaddr);

    return count;
}

void send_udp_broadcast_message(const int redis_port, const int redis_tls) {
    struct ifaddrs *ifaddr;
    const int max_threads = count_usable_interfaces();

    if (!max_threads) {
        RedisModule_Log(
            NULL,
            "notice",
            "%s: no network interfaces found",
            get_broadcast_name()
        );

        return;
    }

    if (getifaddrs(&ifaddr) == -1) {
        RedisModule_Log(
            NULL,
            "error",
            "%s: getifaddrs failed: %s",
            get_broadcast_name(),
            strerror(errno)
        );

        return;
    }

    thread_ids = malloc(sizeof(pthread_t) * max_threads);

    if (!thread_ids) {
        freeifaddrs(ifaddr);
        RedisModule_Log(
            NULL,
            "error",
            "%s: failed to allocate thread array",
            get_broadcast_name()
        );

        return;
    }

    thread_count = 0;

    // the interface list can grow between the count above and this scan
    for (const struct ifaddrs *ifa = ifaddr; ifa && thread_count < max_threads; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        const struct sockaddr_in *addr_in = (struct sockaddr_in *)ifa->ifa_addr;
        char ip[INET_ADDRSTRLEN];

        inet_ntop(AF_INET, &addr_in->sin_addr, ip, sizeof(ip));

        if (!should_broadcast_from_ip(ip)) {
            continue;
        }

        // memory is freed inside the thread
        // ReSharper disable once CppDFAMemoryLeak
        BroadcastTask *task = malloc(sizeof(BroadcastTask));

        if (!task) {
            continue;
        }

        strncpy(task->source_ip, ip, sizeof(task->source_ip));
        task->redis_port = redis_port;
        task->redis_tls = redis_tls;

        pthread_t tid;

        // joinable on purpose: a detached thread outlives MODULE UNLOAD, and
        // on glibc it then wakes up inside code that dlclose() has unmapped
        if (pthread_create(&tid, NULL, broadcast_thread_socket, task) == 0) {
            thread_ids[thread_count++] = tid;
        } else {
            free(task);
        }
    }

    freeifaddrs(ifaddr);
}

/*
 * Tells the broadcasters to say "down" and waits for every one of them, so that
 * none is left running once redis unloads this module.
 */
void cleanup_threads() {
    if (!thread_ids) {
        return;
    }

    is_closing = 1;

    for (int i = 0; i < thread_count; i++) {
        pthread_join(thread_ids[i], NULL);
    }

    free(thread_ids);
    thread_ids = NULL;
    thread_count = 0;
}

// set by the first cron tick after load, reset in RedisModule_OnLoad() so a
// reloaded module announces again
static int announced = 0;

/*
 * Starts the broadcaster threads.
 *
 * Called from the startup cron hook rather than from RedisModule_OnLoad(),
 * because modules are loaded before the listeners exist: announcing there
 * advertised an address that still refused connections, and a client that
 * dialled the address it learned that way was refused.
 */
void start_broadcasting(RedisModuleCtx *ctx) {
    RedisModule_Log(
        ctx,
        "notice",
        "%s: announcing port %d (%s)",
        get_broadcast_name(),
        global_redis_port,
        global_redis_tls ? "tls" : "plain"
    );

    send_udp_broadcast_message(global_redis_port, global_redis_tls);
}

/*
 * Fires on every server cron; announces on the first one after load.
 *
 * Redis enters its event loop only after initListeners() and loadDataFromDisk(),
 * so the first announcement cannot precede the listener.
 *
 * The hook stays subscribed for the life of the module. Unsubscribing from
 * inside the callback frees the listener that moduleFireServerEvent() still
 * dereferences after the callback returns (el->module->in_hook--), a
 * use-after-free on every redis from 6.2 to unstable; redis drops the
 * subscription itself on unload. A flag check per cron tick is the cost.
 */
void cron_broadcast_once(
    RedisModuleCtx *ctx,
    RedisModuleEvent e,
    uint64_t subevent,
    void *data
) {
    (void)e;
    (void)subevent;
    (void)data;

    // a shutdown that wins the race must not start broadcaster threads
    if (announced || is_closing) {
        return;
    }

    announced = 1;
    start_broadcasting(ctx);
}

void shutdown_callback(
    // ReSharper disable once CppParameterMayBeConstPtrOrRef
    RedisModuleCtx *ctx,
    // ReSharper disable once CppParameterMayBeConst
    RedisModuleEvent e,
    // ReSharper disable once CppParameterMayBeConst
    uint64_t subevent,
    // ReSharper disable once CppParameterMayBeConstPtrOrRef
    void *data
) {
    (void)ctx;
    (void)e;
    (void)subevent;
    (void)data;

    is_closing = 1;
    sleep(1);
}

// Redis Module initialization
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    generate_redis_guid();

    if (RedisModule_Init(ctx, "promoter", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    is_closing = 0;
    // a reloaded module must announce again; on musl the DSO is never unloaded,
    // so file-scope state survives MODULE UNLOAD / LOAD unless reset here
    announced = 0;
    load_redis_bind_ips(ctx);
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, shutdown_callback);

    // Determine if we should enable logging based on Redis loglevel
    RedisModuleCallReply *loglevel_reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "loglevel");

    if (loglevel_reply &&
        RedisModule_CallReplyType(loglevel_reply) == REDISMODULE_REPLY_ARRAY &&
        RedisModule_CallReplyLength(loglevel_reply) == 2
    ) {
        RedisModuleCallReply *level = RedisModule_CallReplyArrayElement(loglevel_reply, 1);

        if (level &&
            RedisModule_CallReplyType(level) == REDISMODULE_REPLY_STRING
        ) {
            RedisModuleString *level_str = RedisModule_CreateStringFromCallReply(level);
            size_t len;
            const char *level_cstr = RedisModule_StringPtrLen(level_str, &len);

            if (strcmp(level_cstr, "verbose") == 0 || strcmp(level_cstr, "debug") == 0) {
                enable_logging = 1;
            }

            RedisModule_FreeString(ctx, level_str);
        }
    }

    if (loglevel_reply) {
        RedisModule_FreeCallReply(loglevel_reply);
    }

    global_redis_port = resolve_announced_port(ctx, &global_redis_tls);

    if (global_redis_port <= 0) {
        // a broker that announces nothing is invisible to the whole fleet, so
        // this is a warning and it is the last word on the subject - the reason
        // was logged by resolve_announced_port
        RedisModule_Log(
            ctx,
            "warning",
            "%s: no reachable listener to announce, this broker stays invisible",
            get_broadcast_name()
        );

        return REDISMODULE_OK;
    }

    if (RedisModule_SubscribeToServerEvent(
            ctx, RedisModuleEvent_CronLoop, cron_broadcast_once) != REDISMODULE_OK) {
        // without the hook nothing ever announces, which is the one failure
        // this module must never keep quiet about
        RedisModule_Log(
            ctx,
            "warning",
            "%s: could not register the startup hook, this broker stays invisible",
            get_broadcast_name()
        );
    }

    return REDISMODULE_OK;
}

/*
 * Redis calls this as int (*)(RedisModuleCtx *) and refuses the unload when it
 * returns REDISMODULE_ERR. Without it the broadcasters kept running after the
 * unload: a crash on glibc, a leaked announcer per reload on musl.
 */
int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    (void)ctx;

    cleanup_threads();

    return REDISMODULE_OK;
}
