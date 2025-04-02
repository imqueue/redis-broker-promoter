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
#include "redismodule.h"

// Default values
#define DEFAULT_BROADCAST_ADDRESSES "255.255.255.255"
#define DEFAULT_BROADCAST_PORT 63000
#define DEFAULT_BROADCAST_INTERVAL 1

// Struct to pass data to thread safely
typedef struct {
    int redis_port;
    char broadcast_addresses[256];
} ThreadArgs;

static int enable_logging = 0;
static int global_redis_port = 6379;
static char global_redis_host[INET_ADDRSTRLEN] = {0};

int parse_int(const char *buff) {
    char *end;

    errno = 0;

    const long sl = strtol(buff, &end, 10);

    if (end == buff || '\0' != *end
        || ((LONG_MIN == sl || LONG_MAX == sl) && ERANGE == errno)
        || sl > INT_MAX
        || sl < INT_MIN
    ) {
        return 0;
    }

    return (int)sl;
}

// Function to get local IP address
void get_local_ip(char *buffer, size_t buffer_size) {
    struct ifaddrs *ifaddr;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs failed");
        strncpy(buffer, "UNKNOWN_IP", buffer_size);
        strncpy(global_redis_host, "UNKNOWN_IP", buffer_size);

        return;
    }

    for (const struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) {
          continue;
        }

        const int family = ifa->ifa_addr->sa_family;

        if (family == AF_INET) {
            struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
            inet_ntop(AF_INET, &addr->sin_addr, buffer, buffer_size);
            inet_ntop(AF_INET, &addr->sin_addr, global_redis_host, buffer_size);
            break;
        }
    }

    freeifaddrs(ifaddr);
}

int get_broadcast_port() {
    const char *env = getenv("REDIS_BROADCAST_PORT");

    if (env) {
        int port = parse_int(env);

        if (port > 0 && port <= 65535) {
            return port;
        }
    }

    return DEFAULT_BROADCAST_PORT;  // Default port
}

// Function to send UDP broadcast
void send_udp_broadcast(int redis_port, char *broadcast_addresses) {
    char local_ip[INET_ADDRSTRLEN] = {0};
    const int broadcast_port = get_broadcast_port();

    get_local_ip(local_ip, sizeof(local_ip));

    char message[256];

    snprintf(message, sizeof(message), "imq-broker\tup\t%s:%d", local_ip, redis_port);

    struct sockaddr_in broadcast_addr;
    const int broadcast_enable = 1;
    const int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    if (sockfd < 0) {
        perror("Socket creation failed");

        return;
    }

    if (setsockopt(
        sockfd,
        SOL_SOCKET,
        SO_BROADCAST,
        &broadcast_enable,
        sizeof(broadcast_enable)) < 0
    ) {
        perror("Failed to set broadcast mode");
        close(sockfd);

        return;
    }

    char *token = strtok(broadcast_addresses, ",");

    while (token) {
        memset(&broadcast_addr, 0, sizeof(broadcast_addr));
        broadcast_addr.sin_family = AF_INET;
        broadcast_addr.sin_addr.s_addr = inet_addr(token);
        broadcast_addr.sin_port = htons(broadcast_port);

        if (sendto(
            sockfd,
            message,
            strlen(message),
            0,
            (struct sockaddr *)&broadcast_addr, sizeof(broadcast_addr)
        ) < 0) {
            if (enable_logging) {
                RedisModule_Log(NULL, "warning", "Broadcast to %s failed: %s", token, strerror(errno));
            }
        } else {
            if (enable_logging) {
                RedisModule_Log(NULL, "notice", "UDP Broadcast Sent to %s: %s", token, message);
            }
        }

        token = strtok(NULL, ",");
    }

    close(sockfd);
}

void *broadcast_thread(void *arg) __attribute__((noreturn));

// Background thread function
void *broadcast_thread(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    int redis_port = args->redis_port;
    char broadcast_addresses[256];

    strcpy(broadcast_addresses, args->broadcast_addresses);

    // Get interval from environment variable
    const char *interval_str = getenv("REDIS_BROADCAST_INTERVAL");
    const int broadcast_interval = interval_str ? parse_int(interval_str) : DEFAULT_BROADCAST_INTERVAL;

    while (1) {
        sleep(broadcast_interval);
        send_udp_broadcast(redis_port, broadcast_addresses);
    }
}

// ReSharper disable once CppParameterMayBeConstPtrOrRef, CppParameterMayBeConst
void shutdown_callback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t subevent, void *data) {
    (void)e;
    (void)subevent;
    (void)data;

    char message[256];
    snprintf(message, sizeof(message), "imq-broker\tdown\t%s:%d", global_redis_host, global_redis_port);

    // Get broadcast addresses
    char *broadcast_addresses = getenv("REDIS_BROADCAST_ADDRESSES");

    if (!broadcast_addresses) {
        broadcast_addresses = DEFAULT_BROADCAST_ADDRESSES;
    }

    char addresses[256];
    const int broadcast_port = get_broadcast_port();

    strncpy(addresses, broadcast_addresses, sizeof(addresses));
    addresses[sizeof(addresses) - 1] = '\0';  // Ensure null-termination

    const int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    if (sockfd < 0) {
        return;
    }

    const int broadcast_enable = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable));

    struct sockaddr_in addr;
    char *token = strtok(addresses, ",");

    while (token != NULL) {
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(token);
        addr.sin_port = htons(broadcast_port);

        sendto(sockfd, message, strlen(message), 0, (struct sockaddr *)&addr, sizeof(addr));

        if (enable_logging && ctx) {
            RedisModule_Log(ctx, "notice", "Shutdown broadcast to %s: %s", token, message);
        }

        token = strtok(NULL, ",");
    }

    close(sockfd);
}

// Redis Module initialization
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    if (RedisModule_Init(ctx, "promoter", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, shutdown_callback);

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "port");
    // Determine if we should enable logging based on Redis loglevel
    RedisModuleCallReply *loglevel_reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "loglevel");

    if (loglevel_reply && RedisModule_CallReplyType(loglevel_reply) == REDISMODULE_REPLY_ARRAY &&
        RedisModule_CallReplyLength(loglevel_reply) == 2
    ) {
        RedisModuleCallReply *level = RedisModule_CallReplyArrayElement(loglevel_reply, 1);

        if (level && RedisModule_CallReplyType(level) == REDISMODULE_REPLY_STRING) {
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

    if (reply && RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY &&
        RedisModule_CallReplyLength(reply) == 2
    ) {
        RedisModuleCallReply *port_reply = RedisModule_CallReplyArrayElement(reply, 1);

        if (port_reply && RedisModule_CallReplyType(port_reply) == REDISMODULE_REPLY_STRING) {
            RedisModuleString *port_str = RedisModule_CreateStringFromCallReply(port_reply);
            size_t len;
            const char *port_cstr = RedisModule_StringPtrLen(port_str, &len);
            global_redis_port = parse_int(port_cstr);

            RedisModule_FreeString(ctx, port_str);
        }
    }

    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    // Get broadcast addresses
    char *broadcast_addresses = getenv("REDIS_BROADCAST_ADDRESSES");

    if (!broadcast_addresses) {
        broadcast_addresses = DEFAULT_BROADCAST_ADDRESSES;
    }

    // Allocate memory for thread arguments
    ThreadArgs *args = malloc(sizeof(ThreadArgs));
    args->redis_port = global_redis_port;
    strncpy(args->broadcast_addresses, broadcast_addresses, sizeof(args->broadcast_addresses));

    // Start background thread
    pthread_t thread_id;

    if (pthread_create(&thread_id, NULL, broadcast_thread, args) != 0) {
        free(args);  // Free memory if thread fails
        return REDISMODULE_ERR;
    }

    pthread_detach(thread_id);

    return REDISMODULE_OK;
}
