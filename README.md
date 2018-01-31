# ClickHouse-Bulk
 
[![Build Status](https://travis-ci.org/nikepan/clickhouse-bulk.svg?branch=master)](https://travis-ci.org/nikepan/clickhouse-bulk)
[![codecov](https://codecov.io/gh/nikepan/clickhouse-bulk/branch/master/graph/badge.svg)](https://codecov.io/gh/nikepan/clickhouse-bulk)
[![Go Report Card](https://goreportcard.com/badge/github.com/nikepan/clickhouse-bulk)](https://goreportcard.com/report/github.com/nikepan/clickhouse-bulk)

It is a fork of https://github.com/nikepan/clickhouse-bulk

In this version used more powerful fasthttp server. It is better under high load than echo server

Functionality in this version the same as in original.

Simple [Yandex ClickHouse](https://clickhouse.yandex/) insert collector. It collect requests and send to ClickHouse servers.


### Installation

From sources (Go 1.8+):

```text
git clone github.com/nikepan/clickhouse-bulk
cd clickhouse-bulk
go get
go build
```

You can download linux-64 binary [clickhouse-bulk-linux_64.tgz](https://www.dropbox.com/s/e99urgy3sx71f41/clickhouse-bulk-linux_64.tgz?dl=0)


### Features
- Group n requests and send to any of ClickHouse server
- Sending collected data by interval
- Tested with VALUES, TabSeparated formats
- Supports many servers to send
- Supports query in query parameters and in body
- Supports other query parameters like username, password, database
- Supports basic authentication
 

For example:
```sql
INSERT INTO table3 (c1, c2, c3) VALUES ('v1', 'v2', 'v3')
INSERT INTO table3 (c1, c2, c3) VALUES ('v4', 'v5', 'v6')
```
sends as
```sql
INSERT INTO table3 (c1, c2, c3) VALUES ('v1', 'v2', 'v3')('v4', 'v5', 'v6')
```


### Options
- -config - config file (json); default _config.json_


### Configuration file
```javascript
{
  "listen": ":8124", 
  "flush_count": 10000, // check by \n char
  "flush_interval": 1000, // milliseconds
  "debug": false, // log incoming requests
  "dump_dir": "dumps", // directory for dump unsended data (if clickhouse errors)
  "clickhouse": {
    "down_timeout": 300, // wait if server in down (seconds)
    "servers": [
      "http://127.0.0.1:8123"
    ]
  }
}
```


### Quickstart

`./clickhouse-bulk`
and send queries to :8124


### Tips

For better performance words FORMAT and VALUES must be uppercase.
