from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from benchmark.tpcc.core.result import median_tpmc
from benchmark.tpcc.core.rmdb_backend import RmdbBackend
from benchmark.tpcc.core.sqlite_backend import SqliteBackend
from benchmark.tpcc.phases.benchmark import run_benchmark
from benchmark.tpcc.core.parsing import scalar_int
from benchmark.tpcc.phases.consistency import run_consistency, run_district_diagnostics
from benchmark.tpcc.phases.datagen import (
    complete_csv_set,
    ensure_empty_or_allowed,
    generate_all,
)
from benchmark.tpcc.phases.datagen import (
    complete_csv_set,
    ensure_empty_or_allowed,
    generate_all,
)
from benchmark.tpcc.phases.load import TABLES, execute_sql_file, load_all


def rmdb_backend_factory(args):
    return lambda: RmdbBackend(args.host, args.port, args.timeout)


def sqlite_backend_factory(args):
    return lambda: SqliteBackend(str(args.sqlite_path))


def import_csv_to_sqlite(sqlite_path: Path, data_dir: Path) -> None:
    backend = SqliteBackend(str(sqlite_path))
    try:
        schema_dir = Path(__file__).parent / "schema"
        execute_sql_file(backend, schema_dir / "sqlite_schema.sql")
        execute_sql_file(backend, schema_dir / "sqlite_indexes.sql")
        for table in TABLES:
            csv_path = data_dir / f"{table}.csv"
            with csv_path.open(newline="") as file:
                reader = csv.reader(file)
                header = next(reader)
                placeholders = ",".join(["?"] * len(header))
                insert_sql = f"insert into {table} values ({placeholders})"
                for row in reader:
                    backend.conn.execute(insert_sql, row)
        backend.conn.commit()
    finally:
        backend.close()


def count_orders(backend) -> int:
    return scalar_int(backend.execute("select count(*) from orders;"), 0)


def count_warehouses(backend) -> int:
    return scalar_int(backend.execute("select count(*) from warehouse;"), 0)


def count_districts(backend) -> int:
    return scalar_int(backend.execute("select count(*) from district;"), 0)


def phase(message: str) -> None:
    print(f"[tpcc] {message}", flush=True)


def phase(message: str) -> None:
    print(f"[tpcc] {message}", flush=True)


def max_district_id(backend) -> int:
    return scalar_int(backend.execute("select max(d_id) from district;"), 0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=["datagen", "load", "run", "consistency", "all"]
    )
    parser.add_argument("--backend", choices=["rmdb", "sqlite"], default="rmdb")
    parser.add_argument("--warehouses", type=int, default=1)
    parser.add_argument("--data-dir", type=Path, default=Path("benchmark/tpcc/data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--measure", type=int, default=360)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--progress-interval", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--warehouse-policy",
        choices=["terminal-home", "random-per-txn"],
        default="terminal-home",
    )
    parser.add_argument(
        "--json-out", type=Path, default=Path("benchmark/tpcc/result.json")
    )
    parser.add_argument("--committed-new-order", type=int, default=0)
    parser.add_argument("--baseline-orders-total", type=int, default=-1)
    parser.add_argument(
        "--sqlite-path", type=Path, default=Path("benchmark/tpcc/tpcc.sqlite")
    )
    parser.add_argument("--rmdb-db-dir", type=Path)
    parser.add_argument("--overwrite-data-dir", action="store_true")
    parser.add_argument("--reuse-data-dir", action="store_true")
    parser.add_argument(
        "--skip-consistency",
        action="store_true",
        help="skip the consistency check phase (useful for profiling)",
    )
    args = parser.parse_args()

    schema_dir = Path(__file__).parent / "schema"

    if args.command in ("datagen", "all"):
        if args.reuse_data_dir and complete_csv_set(args.data_dir, TABLES):
            phase(f"datagen skipped, reusing CSV files in {args.data_dir}")
        else:
            phase(
                f"datagen start: warehouses={args.warehouses}, seed={args.seed}, dir={args.data_dir}"
            )
            ensure_empty_or_allowed(args.data_dir, overwrite=args.overwrite_data_dir)
            generate_all(
                args.warehouses,
                args.data_dir,
                args.seed,
                overwrite=args.overwrite_data_dir,
            )
            phase("datagen complete")

    if args.command in ("load", "all"):
        phase(f"load start: backend={args.backend}, data_dir={args.data_dir}")
        if args.backend == "sqlite":
            import_csv_to_sqlite(args.sqlite_path, args.data_dir)
        else:
            backend = rmdb_backend_factory(args)()
            try:
                load_all(backend, args.data_dir, schema_dir, "rmdb", args.rmdb_db_dir)
            finally:
                backend.close()
        phase("load complete")

    rounds = []
    baseline_orders_total = args.baseline_orders_total
    baseline_warehouse_total = -1
    baseline_district_total = -1
    districts_per_warehouse = 0
    if args.command in ("run", "all"):
        factory = (
            sqlite_backend_factory(args)
            if args.backend == "sqlite"
            else rmdb_backend_factory(args)
        )
        baseline_backend = factory()
        try:
            baseline_orders_total = count_orders(baseline_backend)
            baseline_warehouse_total = count_warehouses(baseline_backend)
            baseline_district_total = count_districts(baseline_backend)
            districts_per_warehouse = max_district_id(baseline_backend)
        finally:
            baseline_backend.close()
        phase(
            f"run start: rounds={args.rounds}, workers={args.workers}, "
            f"warmup={args.warmup}s, measure={args.measure}s"
        )
        rounds = run_benchmark(
            factory,
            warehouses=args.warehouses,
            workers=args.workers,
            warmup_seconds=args.warmup,
            measure_seconds=args.measure,
            rounds=args.rounds,
            warehouse_policy=args.warehouse_policy,
            progress_interval=args.progress_interval,
        )
        summary = {
            "config": {
                "backend": args.backend,
                "warehouses": args.warehouses,
                "workers": args.workers,
                "warmup": args.warmup,
                "measure": args.measure,
                "rounds": args.rounds,
                "progress_interval": args.progress_interval,
                "seed": args.seed,
                "warehouse_policy": args.warehouse_policy,
                "data_dir": str(args.data_dir),
                "sqlite_path": str(args.sqlite_path),
                "baseline_warehouse_total": baseline_warehouse_total,
                "baseline_district_total": baseline_district_total,
                "baseline_orders_total": baseline_orders_total,
            },
            "median_tpmc": median_tpmc(rounds),
            "rounds": [round_result.to_dict() for round_result in rounds],
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary, indent=2))
        phase(f"run complete: result={args.json_out}")

    if args.command in ("consistency", "all"):
        if args.command == "all" and args.skip_consistency:
            phase("consistency skipped (--skip-consistency)")
        else:
            phase("consistency start")
            committed = (
                sum(round_result.total_committed_new_order() for round_result in rounds)
                if rounds
                else args.committed_new_order
            )
            backend = (
                sqlite_backend_factory(args)()
                if args.backend == "sqlite"
                else rmdb_backend_factory(args)()
            )
            try:
                if baseline_orders_total < 0:
                    current_orders = count_orders(backend)
                    baseline_orders_total = current_orders - committed
                if baseline_warehouse_total < 0:
                    baseline_warehouse_total = count_warehouses(backend)
                if baseline_district_total < 0:
                    baseline_district_total = count_districts(backend)
                if districts_per_warehouse <= 0:
                    districts_per_warehouse = max_district_id(backend)
                failures = run_consistency(
                    backend,
                    baseline_warehouse_total,
                    baseline_district_total,
                    baseline_orders_total,
                    committed,
                )
                failures.extend(
                    run_district_diagnostics(
                        backend, baseline_warehouse_total, districts_per_warehouse
                    )
                )
            finally:
                backend.close()
            if failures:
                raise SystemExit("\n".join(failures))
            print("consistency ok")
            phase("consistency complete")


if __name__ == "__main__":
    main()
