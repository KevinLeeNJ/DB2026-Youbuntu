# rmdb TPC-C Benchmark

Local Go-based TPC-C benchmark tooling for RMDB.

The RMDB benchmark defaults to `READ COMMITTED`. Use
`TPCC_ISOLATION=snapshot-isolation` when invoking the script directly to run
the same data set and worker count under snapshot isolation.

The default `make benchmark` path uses the Go load generator for the run phase:

```bash
make benchmark TPCC_WORKERS=16 TPCC_WARMUP=10 TPCC_MEASURE=60
```

It keeps one TCP connection per worker and uses goroutines with independent
random generators. The same binary also generates CSV data, loads the RMDB
schema/data, waits for recovery, merges round results, and runs consistency
checks. `make benchmark` therefore has no Python runtime dependency.
