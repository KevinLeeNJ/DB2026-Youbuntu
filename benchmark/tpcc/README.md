# rmdb TPC-C Benchmark

Local TPC-C benchmark tooling for rmdb, with an optional SQLite baseline for rough throughput comparison.

Default rmdb flow:

```bash
python3 -m benchmark.tpcc.tpcc_run all --warehouses 1 --data-dir benchmark/tpcc/data --host 127.0.0.1 --port 8765
```

The RMDB benchmark defaults to the server's `READ COMMITTED` isolation level.
Use `--isolation snapshot-isolation` to set snapshot isolation on every benchmark
connection, so RC and SI runs use the same binary, data set, worker count, and
duration. The selected level is recorded in the result JSON.

The default `make benchmark` path uses the Go load generator for the run phase:

```bash
make benchmark TPCC_WORKERS=16 TPCC_WARMUP=10 TPCC_MEASURE=60
```

It keeps one TCP connection per worker and uses goroutines with independent
random generators. Python remains responsible for data generation, loading,
and post-crash consistency validation, so the existing result JSON and
validation workflow remain compatible.
