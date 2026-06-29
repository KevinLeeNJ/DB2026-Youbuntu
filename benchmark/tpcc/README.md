# rmdb TPC-C Benchmark

Local TPC-C benchmark tooling for rmdb, with an optional SQLite baseline for rough throughput comparison.

Default rmdb flow:

```bash
python3 -m benchmark.tpcc.tpcc_run all --warehouses 1 --data-dir benchmark/tpcc/data --host 127.0.0.1 --port 8765
```

The rmdb benchmark uses the server default isolation level and does not send `SET TRANSACTION ISOLATION LEVEL`.

