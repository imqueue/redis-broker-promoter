# Promoter

UDP broadcasting server node on redis start using custom promoter module

## Build

To build a module use:

```bash
make
```

to clean:

```bash
make clean
```

See all available targets in Makefile (install/uninstall/reload/run etc.).

## Usage

Environment variables available:

 - REDIS_BROADCAST_NAME (default is "imq-broker")
 - REDIS_BROADCAST_PORT (default is 63000)
 - REDIS_BROADCAST_INTERVAL (in seconds, default is 1 second)

Either configure through redis.conf or by launching the server with `--loadmodule` option, like:

```bash
redis-server --loadmodule /path/to/promoter.so
```

If you need to log messages and errors from module, enable by:

```bash
redis-server --loadmodule /path/to/promoter.so --loglevel verbose
```

This will work at the moment with IPv4 only. If redis is bound to 0.0.0.0 message will be broadcast to all available 
network interfaces. In this case either the client must take care about listening the specific interface or manage
provided messages by redis GUID provided within the message to handle it as a single instance if needed.

Message format on redis running is:

```aiignore
[REDIS_BROADCAST_NAME]  [REDIS_GUID]    [STATUS]    [REDIS_INTERFACE_HOST]:[REDIS_PORT]   <REDIS_BROADCAST_INTERVAL>
```

Where STATUS could be one of "up" or "down", and REDIS_BROADCAST_INTERVAL only present if STATUS is "up", e.g:

```aiignore
imq-broker      2cc7c345-3569-44bb-b57a-b72d729d7012    up      127.0.0.1:6380  1
imq-broker      2cc7c345-3569-44bb-b57a-b72d729d7012    up      127.0.0.1:6380  1
imq-broker      2cc7c345-3569-44bb-b57a-b72d729d7012    down    127.0.0.1:6380
```