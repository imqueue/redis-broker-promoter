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

 - REDIS_BROADCAST_ADDRESSES (default is "255.255.255.255")
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
