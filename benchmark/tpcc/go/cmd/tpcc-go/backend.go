package main

// txnBackend is the small common surface exercised by the TPC-C transaction
// implementations. RMDB and SQLite intentionally share the same SQL strings
// so the comparison measures the engines, not two different workloads.
type txnBackend interface {
	exec(sql string) (string, error)
	begin() error
	commit() error
	rollback()
	close()
}

type backendFactory func() (txnBackend, error)
