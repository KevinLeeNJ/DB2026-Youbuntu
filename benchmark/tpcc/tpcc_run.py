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
        # Build tables first and defer indexes until all rows are loaded. The
        # previous path created indexes before inserting and autocommitted each
        # row through sqlite3's isolation_level=None connection.
        execute_sql_file(backend, schema_dir / "sqlite_schema.sql")
        backend.conn.execute("begin;")
        for table in TABLES:
            csv_path = data_dir / f"{table}.csv"
            with csv_path.open(newline="") as file:
                reader = csv.reader(file)
                header = next(reader)
                placeholders = ",".join(["?"] * len(header))
                insert_sql = f"insert into {table} values ({placeholders})"
                backend.conn.executemany(insert_sql, reader)
        backend.conn.commit()
        execute_sql_file(backend, schema_dir / "sqlite_indexes.sql")
    except Exception:
        backend.conn.rollback()
        raise
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


def load_baseline_and_committed_from_result(
    result_path: Path,
) -> tuple[int, int, int, int]:
    """Read baseline counts and total committed new_order from a result.json.

    Used by the standalone consistency subcommand after a crash/restart so it
    does not have to re-derive these values from an in-memory `rounds` list or
    by re-querying the (now recovered) server. Returns
    (baseline_warehouse_total, baseline_district_total, baseline_orders_total,
     total_committed_new_order). Raises SystemExit if the file is missing or
    lacks the required fields.
    """
    if not result_path.exists():
        raise SystemExit(f"--result-json 指向的文件不存在: {result_path}")
    data = json.loads(result_path.read_text())
    config = data.get("config", {})
    try:
        baseline_warehouse = int(config["baseline_warehouse_total"])
        baseline_district = int(config["baseline_district_total"])
        baseline_orders = int(config["baseline_orders_total"])
    except KeyError as exc:
        raise SystemExit(f"{result_path} 缺少 baseline 字段: {exc}") from exc
    committed = 0
    for round_result in data.get("rounds", []):
        counts = round_result.get("counts", {})
        for phase in ("warmup", "measure", "drain"):
            committed += int(
                counts.get(phase, {}).get("new_order", {}).get("commit", 0)
            )
    return baseline_warehouse, baseline_district, baseline_orders, committed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        nargs="+",
        choices=["datagen", "load", "run", "consistency", "all"],
        help="one or more phases to run in order: datagen, load, run, consistency, all",
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
    parser.add_argument(
        "--result-json",
        type=Path,
        default=None,
        help="read baseline counts and committed new_order total from a result.json "
        "(written by the run phase); used by the standalone consistency subcommand "
        "after a crash/restart so it does not have to re-derive them in memory",
    )
    parser.add_argument("--overwrite-data-dir", action="store_true")
    parser.add_argument("--reuse-data-dir", action="store_true")
    parser.add_argument(
        "--skip-consistency",
        action="store_true",
        help="skip the consistency check phase (useful for profiling)",
    )
    parser.add_argument(
        "--consistency-stage",
        default="standalone",
        help="stage label used by the standalone consistency command",
    )
    args = parser.parse_args()

    schema_dir = Path(__file__).parent / "schema"
    rounds = []
    baseline_orders_total = args.baseline_orders_total
    baseline_warehouse_total = -1
    baseline_district_total = -1
    districts_per_warehouse = 0

    def validate(
        stage: str,
        warehouse_total: int,
        district_total: int,
        district_count: int,
    ) -> None:
        phase(f"consistency start: stage={stage}")
        backend = (
            sqlite_backend_factory(args)()
            if args.backend == "sqlite"
            else rmdb_backend_factory(args)()
        )
        try:
            failures = run_consistency(
                backend,
                warehouse_total,
                district_total,
            )
            failures.extend(
                run_district_diagnostics(backend, warehouse_total, district_count)
            )
        finally:
            backend.close()
        if failures:
            details = "\n".join(f"[{stage}] {failure}" for failure in failures)
            raise SystemExit(
                f"[{stage}] consistency validation failed ({len(failures)} rule(s))\n{details}"
            )
        print(f"consistency ok: stage={stage}")
        phase(f"consistency complete: stage={stage}")

    if set(args.command) & {"datagen", "all"}:
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

    if set(args.command) & {"load", "all"}:
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
        if args.skip_consistency:
            phase("consistency skipped: stage=post-load (--skip-consistency)")
        else:
            load_backend = (
                sqlite_backend_factory(args)()
                if args.backend == "sqlite"
                else rmdb_backend_factory(args)()
            )
            try:
                baseline_orders_total = count_orders(load_backend)
                baseline_warehouse_total = count_warehouses(load_backend)
                baseline_district_total = count_districts(load_backend)
                districts_per_warehouse = max_district_id(load_backend)
            finally:
                load_backend.close()
            validate(
                "post-load",
                baseline_warehouse_total,
                baseline_district_total,
                districts_per_warehouse,
            )

    if set(args.command) & {"run", "all"}:
        factory = (
            sqlite_backend_factory(args)
            if args.backend == "sqlite"
            else rmdb_backend_factory(args)
        )
        baseline_backend = factory()
        try:
            # Disable output.txt writes for the entirety of the run phase (counting
            # probe + benchmark rounds + consistency). "set output_file off" is a
            # server-wide toggle on SmManager, so one call here covers every
            # subsequent connection in this process. This keeps throughput
            # measurement from appending query results to output.txt.
            if args.backend == "rmdb":
                try:
                    baseline_backend.execute("set output_file off")
                except Exception:
                    pass
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
        if args.skip_consistency:
            phase("consistency skipped: stage=post-transaction (--skip-consistency)")
        else:
            validate(
                "post-transaction",
                baseline_warehouse_total,
                baseline_district_total,
                districts_per_warehouse,
            )

    if "consistency" in args.command:
        if args.skip_consistency:
            phase(
                f"consistency skipped: stage={args.consistency_stage} (--skip-consistency)"
            )
        else:
            if args.result_json is not None:
                (
                    baseline_warehouse_total,
                    baseline_district_total,
                    _,
                    _,
                ) = load_baseline_and_committed_from_result(args.result_json)
                phase(
                    f"loaded from {args.result_json}: "
                    f"baseline warehouses={baseline_warehouse_total}, "
                    f"districts={baseline_district_total}"
                )
            backend = (
                sqlite_backend_factory(args)()
                if args.backend == "sqlite"
                else rmdb_backend_factory(args)()
            )
            try:
                if baseline_warehouse_total < 0:
                    baseline_warehouse_total = count_warehouses(backend)
                if baseline_district_total < 0:
                    baseline_district_total = count_districts(backend)
                if districts_per_warehouse <= 0:
                    districts_per_warehouse = max_district_id(backend)
            finally:
                backend.close()
            validate(
                args.consistency_stage,
                baseline_warehouse_total,
                baseline_district_total,
                districts_per_warehouse,
            )


if __name__ == "__main__":
    main()
