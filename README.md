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
 - REDIS_BROADCAST_TLS (unset by default, see "TLS" below)

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
[REDIS_BROADCAST_NAME]  [REDIS_GUID]    [STATUS]    [REDIS_INTERFACE_HOST]:[REDIS_PORT]   <REDIS_BROADCAST_INTERVAL>  <TRANSPORT>
```

Where STATUS could be one of "up" or "down", and REDIS_BROADCAST_INTERVAL and TRANSPORT only present if STATUS is
"up". TRANSPORT is "tls" when the announced port is the TLS listener and "plain" when it is not — see "TLS" below, e.g:

```aiignore
imq-broker      2cc7c345-3569-44bb-b57a-b72d729d7012    up      127.0.0.1:6380  1   plain
imq-broker      2cc7c345-3569-44bb-b57a-b72d729d7012    up      127.0.0.1:6380  1   plain
imq-broker      2cc7c345-3569-44bb-b57a-b72d729d7012    down    127.0.0.1:6380
```

The fields are positional and only ever appended to, so a reader that splits on tabs and takes the first five sees
exactly what it saw before this field existed.

## TLS

The announced port is the one the server is actually **listening** on. Redis
serves TLS by setting `port 0` and `tls-port <n>`, so a TLS broker used to
announce `<ip>:0` — an address nothing can connect to, and one @imqueue's UDP
listener discards as malformed. Such a fleet discovered no broker at all, with
nothing in any log to say why: the announcement went out, it was just useless.
The module now announces `tls-port` in that case, and marks the datagram `tls`.

When **both** listeners are up, the plaintext port is announced. That is what
an existing fleet is already connecting to, and upgrading this module must not
move it onto a transport its clients are not configured for. Set
`REDIS_BROADCAST_TLS=1` to announce the TLS port instead; `0` pins plaintext.

If the requested listener is not up — `REDIS_BROADCAST_TLS=1` with no
`tls-port`, or a server listening nowhere — **nothing is announced**, and the
reason is logged at `warning`. Announcing a port that refuses connections, or
quietly downgrading a fleet that asked for TLS, are both worse than being
missing.

The marker describes the announced port and nothing else. It carries no
certificate and configures no client: services still supply their own TLS
options (`IMQ_REDIS_TLS*` in `@imqueue/core`).
