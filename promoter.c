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
#include <uuid/uuid.h>
#include "redismodule.h"

// Default values
#define DEFAULT_BROADCAST_ADDRESSES "255.255.255.255"
#define DEFAULT_BROADCAST_PORT 63000
#define DEFAULT_BROADCAST_INTERVAL 1
#define MAX_REDIS_BINDS 8

// Struct to pass data to thread safely
typedef struct {
    int redis_port;
    char broadcast_addresses[256];
} ThreadArgs;

static int enable_logging = 0;
static int global_redis_port = 6379;

static char redis_guid[37];

static char redis_bind_ips[MAX_REDIS_BINDS][INET_ADDRSTRLEN];
static int redis_bind_count = 0;
static int allow_all_interfaces = 0;

void generate_redis_guid() {
    uuid_t binuuid;
    uuid_generate(binuuid);
    uuid_unparse_lower(binuuid, redis_guid);
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
        if (strcmp(ip, redis_bind_ips[i]) == 0) {
            return 1;
        }
    }

    return 0;
}

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
int get_ip_for_broadcast(const char *broadcast_target, char *output_ip, const size_t output_size) {
    struct ifaddrs *ifaddr;
    struct in_addr target;

    if (inet_aton(broadcast_target, &target) == 0) {
        return -1;  // Invalid broadcast IP
    }

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs failed");
        return -1;
    }

    for (struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }

        const struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
        const struct sockaddr_in *netmask = (struct sockaddr_in *)ifa->ifa_netmask;

        if (!netmask) {
            continue;
        }

        struct in_addr subnet;
        subnet.s_addr = addr->sin_addr.s_addr & netmask->sin_addr.s_addr;
        struct in_addr broadcast;
        broadcast.s_addr = subnet.s_addr | ~netmask->sin_addr.s_addr;

        if (broadcast.s_addr == target.s_addr) {
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
        int port = parse_int(env);

        if (port > 0 && port <= 65535) {
            return port;
        }
    }

    return DEFAULT_BROADCAST_PORT;  // Default port
}

// Function to send UDP broadcast
void send_udp_broadcast(const int redis_port, char *broadcast_addresses, const int broadcast_interval) {
    const int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    if (sockfd < 0) {
        perror("socket creation failed");
        return;
    }

    const int broadcast_enable = 1;

    setsockopt(
        sockfd,
        SOL_SOCKET,
        SO_BROADCAST,
        &broadcast_enable,
        sizeof(broadcast_enable)
    );

    struct sockaddr_in addr;
    char *token = strtok(broadcast_addresses, ",");

    while (token) {
        if (strcmp(token, "255.255.255.255") == 0) {
            // Special handling: broadcast once per interface
            struct ifaddrs *ifaddr;

            if (getifaddrs(&ifaddr) == -1) {
                freeifaddrs(ifaddr);
                perror("imq-broker: getifaddrs failed in shutdown");

                return;
            }

            for (const struct ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
                if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
                    continue;
                }

                const struct sockaddr_in *addr_in = (struct sockaddr_in *)ifa->ifa_addr;
                char ip[INET_ADDRSTRLEN];

                inet_ntop(AF_INET, &addr_in->sin_addr, ip, sizeof(ip));

                if (!should_broadcast_from_ip(ip)) {
                    continue;
                }

                char message[256];

                snprintf(
                    message,
                    sizeof(message),
                    "imq-broker\t%s\tup\t%s:%d\t%d",
                    redis_guid,
                    ip,
                    redis_port,
                    broadcast_interval
                );

                const int s = socket(AF_INET, SOCK_DGRAM, 0);

                if (s < 0) {
                    perror("imq-broker: socket failed for interface");

                    continue;
                }

                int enable = 1;

                setsockopt(s, SOL_SOCKET, SO_BROADCAST, &enable, sizeof(enable));

                struct sockaddr_in dest = {0};

                dest.sin_family = AF_INET;
                dest.sin_addr.s_addr = inet_addr("255.255.255.255");
                dest.sin_port = htons(DEFAULT_BROADCAST_PORT);

                if (sendto(
                    s,
                    message,
                    strlen(message),
                    0,
                    (struct sockaddr *)&dest,
                    sizeof(dest)
                ) < 0) {
                    perror("imq-broker: broadcast message failed");
                } else if (enable_logging) {
                    RedisModule_Log(
                        NULL,
                        "notice",
                        "imq-broadcast: UDP Broadcast from %s: %s",
                        ip,
                        message
                    );
                }

                close(s);
            }

            freeifaddrs(ifaddr);
        } else {
            char interface_ip[INET_ADDRSTRLEN] = "UNKNOWN";

            if (get_ip_for_broadcast(token, interface_ip, sizeof(interface_ip)) != 0) {
                fprintf(stderr, "imq-broker: no matching interface for %s\n", token);
                token = strtok(NULL, ",");

                continue;
            }

            // Compose the message with the correct interface IP
            char message[256];

            snprintf(
                message,
                sizeof(message),
                "imq-broker\t%s\tup\t%s:%d\t%d",
                redis_guid,
                interface_ip,
                redis_port,
                broadcast_interval
            );

            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = inet_addr(token);
            addr.sin_port = htons(DEFAULT_BROADCAST_PORT);

            if (sendto(
                sockfd,
                message,
                strlen(message),
                0,
                (struct sockaddr *)&addr,
                sizeof(addr)
            ) < 0) {
                perror("imq-broker: broadcast message failed");
            } else if (enable_logging) {
                RedisModule_Log(
                    NULL,
                    "notice",
                    "imq-broker: UDP Broadcast Sent to %s: %s",
                    token,
                    message
                );
            }
        }

        token = strtok(NULL, ",");
        close(sockfd);
    }
}

// ReSharper disable once CppDFAConstantFunctionResult
void *broadcast_thread(void *arg) {
    const ThreadArgs *args = (ThreadArgs *)arg;
    const int redis_port = args->redis_port;
    char broadcast_addresses[256];

    strcpy(broadcast_addresses, args->broadcast_addresses);

    // Get interval from environment variable
    const char *interval_str = getenv("REDIS_BROADCAST_INTERVAL");
    const int broadcast_interval = interval_str ? parse_int(interval_str) : DEFAULT_BROADCAST_INTERVAL;

    // ReSharper disable once CppDFAEndlessLoop
    while (1) {
        send_udp_broadcast(redis_port, broadcast_addresses, broadcast_interval);
        sleep(broadcast_interval);
    }
}

void shutdown_callback(
    RedisModuleCtx *ctx,
    // ReSharper disable once CppParameterMayBeConst
    RedisModuleEvent e,
    // ReSharper disable once CppParameterMayBeConst
    uint64_t subevent,
    // ReSharper disable once CppParameterMayBeConstPtrOrRef
    void *data
) {
    (void)e;
    (void)subevent;
    (void)data;

    const char *broadcast_addresses_env = getenv("REDIS_BROADCAST_ADDRESSES");
    const char *fallback = "255.255.255.255";
    const char *addresses_raw = broadcast_addresses_env ? broadcast_addresses_env : fallback;
    char addresses[256];

    strncpy(addresses, addresses_raw, sizeof(addresses));
    addresses[sizeof(addresses) - 1] = '\0';

    char *token = strtok(addresses, ",");

    while (token) {
        if (strcmp(token, "255.255.255.255") == 0) {
            struct ifaddrs *ifaddr;

            if (getifaddrs(&ifaddr) == -1) {
                freeifaddrs(ifaddr);
                perror("imq-broker: getifaddrs failed in shutdown");

                return;
            }

            for (const struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
                if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
                    continue;
                }

                struct sockaddr_in *addr_in = (struct sockaddr_in *)ifa->ifa_addr;
                char ip[INET_ADDRSTRLEN];

                inet_ntop(AF_INET, &addr_in->sin_addr, ip, sizeof(ip));

                if (!should_broadcast_from_ip(ip)) {
                    continue;
                }

                char message[256];

                snprintf(
                    message,
                    sizeof(message),
                    "imq-broker\t%s\tdown\t%s:%d",
                    redis_guid,
                    ip,
                    global_redis_port
                );

                const int s = socket(AF_INET, SOCK_DGRAM, 0);

                if (s < 0) {
                    continue;
                }

                int broadcast = 1;
                setsockopt(s, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

                struct sockaddr_in dest = {0};

                dest.sin_family = AF_INET;
                dest.sin_addr.s_addr = inet_addr("255.255.255.255");
                dest.sin_port = htons(DEFAULT_BROADCAST_PORT);

                sendto(s, message, strlen(message), 0, (struct sockaddr *)&dest, sizeof(dest));
                close(s);

                if (ctx && enable_logging) {
                    RedisModule_Log(ctx, "notice", "Shutdown broadcast from %s to 255.255.255.255", ip);
                }
            }

            freeifaddrs(ifaddr);
        } else {
            // 🔁 Match specific subnet interface
            char ip[INET_ADDRSTRLEN] = "UNKNOWN";

            if (get_ip_for_broadcast(token, ip, sizeof(ip)) != 0) {
                token = strtok(NULL, ",");
                continue;
            }

            char message[256];

            snprintf(
                message,
                sizeof(message),
                "imq-broker\t%s\tdown\t%s:%d",
                redis_guid,
                ip,
                global_redis_port
            );

            int s = socket(AF_INET, SOCK_DGRAM, 0);

            if (s < 0) {
                continue;
            }

            int broadcast = 1;

            setsockopt(s, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

            struct sockaddr_in dest = {0};

            dest.sin_family = AF_INET;
            dest.sin_addr.s_addr = inet_addr(token);
            dest.sin_port = htons(DEFAULT_BROADCAST_PORT);

            sendto(s, message, strlen(message), 0, (struct sockaddr *)&dest, sizeof(dest));
            close(s);

            if (ctx && enable_logging) {
                RedisModule_Log(ctx, "notice", "Shutdown broadcast to %s from %s", token, ip);
            }
        }

        token = strtok(NULL, ",");
    }
}

// Redis Module initialization
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    generate_redis_guid();

    if (RedisModule_Init(ctx, "promoter", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    load_redis_bind_ips(ctx);

    printf("bound ips: ");

    for (int i = 0; i < MAX_REDIS_BINDS; i++) {
        printf("%s", redis_bind_ips[i]);
    }

    printf("\n");

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
