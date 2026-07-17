# rmdb TPC-C Benchmark

Local Go-based TPC-C benchmark tooling for RMDB.

The RMDB benchmark defaults to `READ COMMITTED`. Use
`TPCC_ISOLATION=snapshot-isolation` when invoking the script directly to run
the same data set and worker count under snapshot isolation.

The default `make benchmark` path uses the Go load generator for the RMDB run phase:

```bash
make benchmark TPCC_WORKERS=16 TPCC_WARMUP=10 TPCC_MEASURE=60
```

It keeps one TCP connection per worker and uses goroutines with independent
random generators. The same binary also generates CSV data, loads the RMDB
schema/data, waits for recovery, merges round results, and runs consistency
checks. `make benchmark` therefore has no Python runtime dependency.

SQLite can be run with the same transaction generator and CSV data:

```bash
make benchmark-sqlite TPCC_WARMUP=10 TPCC_MEASURE=60 TPCC_ROUNDS=1
```

The SQLite path uses one connection per worker, WAL mode, and
`synchronous=NORMAL`; it defaults to `BEGIN IMMEDIATE` to avoid turning
read-then-write TPC-C transactions into `database is locked` failures. Use
`--sqlite-begin deferred` to measure SQLite's deferred-lock behavior explicitly.
It writes `benchmark/tpcc/result-sqlite.json`; each result also contains
`txn_tpm` and `latency_ms` entries for `new_order`, `payment`, `order_status`,
`delivery`, and `stock_level`.
