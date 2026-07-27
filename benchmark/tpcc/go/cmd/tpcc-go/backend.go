package main

// txnBackend is the small common surface exercised by the TPC-C transaction
// implementations. RMDB and SQLite intentionally share the same SQL strings
// so the comparison measures the engines, not two different workloads.
type txnBackend interface {
	sqlExecutor
	begin() error
	commit() error
	rollback()
	close()
}

// sqlExecutor is the single method the load-time validation queries need, which
// keeps those checks testable without a live server.
type sqlExecutor interface {
	exec(sql string) (string, error)
}

type backendFactory func() (txnBackend, error)
