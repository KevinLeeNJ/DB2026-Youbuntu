package main

/*
#cgo LDFLAGS: -l:libsqlite3.so.0
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef void (*sqlite3_destructor_type)(void *);

extern int sqlite3_open_v2(const char *, sqlite3 **, int, const char *);
extern int sqlite3_close(sqlite3 *);
extern const char *sqlite3_errmsg(sqlite3 *);
extern int sqlite3_exec(sqlite3 *, const char *, int (*)(void *, int, char **, char **), void *, char **);
extern char *sqlite3_mprintf(const char *, ...);
extern void sqlite3_free(void *);
extern int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
extern int sqlite3_bind_parameter_count(sqlite3_stmt *);
extern int sqlite3_bind_text(sqlite3_stmt *, int, const char *, int, sqlite3_destructor_type);
extern int sqlite3_step(sqlite3_stmt *);
extern int sqlite3_reset(sqlite3_stmt *);
extern int sqlite3_clear_bindings(sqlite3_stmt *);
extern int sqlite3_finalize(sqlite3_stmt *);

#define SQLITE_OK 0
#define SQLITE_ERROR 1
#define SQLITE_IOERR 10
#define SQLITE_CANTOPEN 14
#define SQLITE_MISMATCH 20
#define SQLITE_NOMEM 7
#define SQLITE_ROW 100
#define SQLITE_DONE 101
#define SQLITE_OPEN_READWRITE 0x00000002
#define SQLITE_OPEN_CREATE 0x00000004
#define SQLITE_OPEN_FULLMUTEX 0x00010000
#define SQLITE_TRANSIENT ((sqlite3_destructor_type)-1)

static void set_error(char **target, const char *message) {
    if (target != NULL) {
        *target = sqlite3_mprintf("%s", message == NULL ? "sqlite error" : message);
    }
}

static int open_sqlite(const char *path, sqlite3 **db, char **error_message) {
    int rc = sqlite3_open_v2(path, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
    if (rc != SQLITE_OK) {
        set_error(error_message, *db == NULL ? "sqlite open failed" : sqlite3_errmsg(*db));
        if (*db != NULL) {
            sqlite3_close(*db);
            *db = NULL;
        }
    }
    return rc;
}

static int collect_callback(void *opaque, int argc, char **argv, char **column_names) {
    (void)column_names;
    struct collector {
        char *data;
        size_t length;
    };
    struct collector *collector = (struct collector *)opaque;
    size_t extra = 1;
    for (int i = 0; i < argc; ++i) {
        extra += strlen(argv[i] == NULL ? "" : argv[i]);
        if (i + 1 < argc) {
            extra += 1;
        }
    }
    char *next = (char *)realloc(collector->data, collector->length + extra + 1);
    if (next == NULL) {
        return 1;
    }
    collector->data = next;
    for (int i = 0; i < argc; ++i) {
        const char *value = argv[i] == NULL ? "" : argv[i];
        size_t length = strlen(value);
        memcpy(collector->data + collector->length, value, length);
        collector->length += length;
        if (i + 1 < argc) {
            collector->data[collector->length++] = '|';
        }
    }
    collector->data[collector->length++] = '\n';
    collector->data[collector->length] = '\0';
    return 0;
}

static int exec_collect(sqlite3 *db, const char *sql, char **result, char **error_message) {
    struct collector {
        char *data;
        size_t length;
    } collector = {NULL, 0};
    char *sqlite_error = NULL;
    int rc = sqlite3_exec(db, sql, collect_callback, &collector, &sqlite_error);
    if (rc != SQLITE_OK) {
        if (error_message != NULL) {
            *error_message = sqlite_error == NULL ? sqlite3_mprintf("%s", sqlite3_errmsg(db)) : sqlite_error;
        } else if (sqlite_error != NULL) {
            sqlite3_free(sqlite_error);
        }
        free(collector.data);
        return rc;
    }
    if (result != NULL) {
        *result = collector.data;
    } else {
        free(collector.data);
    }
    return SQLITE_OK;
}

static void free_sqlite_string(char *value) {
    sqlite3_free(value);
}

static char *copy_field(const char *start, size_t length) {
    char *field = (char *)malloc(length + 1);
    if (field == NULL) {
        return NULL;
    }
    memcpy(field, start, length);
    field[length] = '\0';
    return field;
}

// The generated TPC-C CSV files have no embedded newlines. This parser still
// handles quoted fields and escaped double quotes, which is enough for normal
// CSV input without making the benchmark depend on a command-line sqlite tool.
static int parse_csv_line(char *line, char ***fields_out, int *count_out) {
    char **fields = NULL;
    int count = 0;
    char *cursor = line;
    while (*cursor != '\0' && *cursor != '\n' && *cursor != '\r') {
        char *field = NULL;
        size_t capacity = strlen(cursor) + 1;
        if (*cursor == '"') {
            ++cursor;
            char *value = (char *)malloc(capacity);
            if (value == NULL) {
                goto oom;
            }
            size_t length = 0;
            while (*cursor != '\0' && *cursor != '\n' && *cursor != '\r') {
                if (*cursor == '"') {
                    if (cursor[1] == '"') {
                        value[length++] = '"';
                        cursor += 2;
                        continue;
                    }
                    ++cursor;
                    break;
                }
                value[length++] = *cursor++;
            }
            value[length] = '\0';
            field = value;
            if (*cursor == ',') {
                ++cursor;
            }
        } else {
            char *start = cursor;
            while (*cursor != '\0' && *cursor != ',' && *cursor != '\n' && *cursor != '\r') {
                ++cursor;
            }
            field = copy_field(start, (size_t)(cursor - start));
            if (field == NULL) {
                goto oom;
            }
            if (*cursor == ',') {
                ++cursor;
            }
        }
        char **next = (char **)realloc(fields, (size_t)(count + 1) * sizeof(char *));
        if (next == NULL) {
            free(field);
            goto oom;
        }
        fields = next;
        fields[count++] = field;
    }
    *fields_out = fields;
    *count_out = count;
    return 0;

oom:
    for (int i = 0; i < count; ++i) {
        free(fields[i]);
    }
    free(fields);
    return SQLITE_NOMEM;
}

static void free_csv_fields(char **fields, int count) {
    for (int i = 0; i < count; ++i) {
        free(fields[i]);
    }
    free(fields);
}

static int import_csv(sqlite3 *db, const char *table, const char *path, char **error_message) {
    FILE *file = fopen(path, "rb");
    if (file == NULL) {
        char message[512];
        snprintf(message, sizeof(message), "cannot open CSV %s: %s", path, strerror(errno));
        set_error(error_message, message);
        return SQLITE_CANTOPEN;
    }
    char *line = NULL;
    size_t line_capacity = 0;
    ssize_t line_length = getline(&line, &line_capacity, file);
    if (line_length < 0) {
        free(line);
        fclose(file);
        return SQLITE_OK;
    }

    char *insert_sql = NULL;
    sqlite3_stmt *statement = NULL;
    int rc = SQLITE_OK;
    while ((line_length = getline(&line, &line_capacity, file)) >= 0) {
        if (line_length <= 1) {
            continue;
        }
        char **fields = NULL;
        int field_count = 0;
        rc = parse_csv_line(line, &fields, &field_count);
        if (rc != SQLITE_OK) {
            set_error(error_message, "out of memory while parsing CSV");
            break;
        }
        if (field_count == 0) {
            free_csv_fields(fields, field_count);
            continue;
        }
        if (statement == NULL) {
            size_t sql_length = strlen(table) + (size_t)field_count * 3 + 32;
            insert_sql = (char *)malloc(sql_length);
            if (insert_sql == NULL) {
                free_csv_fields(fields, field_count);
                set_error(error_message, "out of memory while preparing CSV import");
                rc = SQLITE_NOMEM;
                break;
            }
            int offset = snprintf(insert_sql, sql_length, "INSERT INTO %s VALUES (", table);
            for (int i = 0; i < field_count; ++i) {
                offset += snprintf(insert_sql + offset, sql_length - (size_t)offset, "%s?", i == 0 ? "" : ",");
            }
            snprintf(insert_sql + offset, sql_length - (size_t)offset, ");");
            rc = sqlite3_prepare_v2(db, insert_sql, -1, &statement, NULL);
            if (rc != SQLITE_OK) {
                set_error(error_message, sqlite3_errmsg(db));
                free_csv_fields(fields, field_count);
                break;
            }
        }
        if (field_count != sqlite3_bind_parameter_count(statement)) {
            set_error(error_message, "CSV column count does not match SQLite table");
            free_csv_fields(fields, field_count);
            rc = SQLITE_MISMATCH;
            break;
        }
        for (int i = 0; i < field_count; ++i) {
            sqlite3_bind_text(statement, i + 1, fields[i], -1, SQLITE_TRANSIENT);
        }
        rc = sqlite3_step(statement);
        if (rc != SQLITE_DONE) {
            set_error(error_message, sqlite3_errmsg(db));
            free_csv_fields(fields, field_count);
            break;
        }
        sqlite3_reset(statement);
        sqlite3_clear_bindings(statement);
        free_csv_fields(fields, field_count);
    }
    if (ferror(file) && rc == SQLITE_OK) {
        set_error(error_message, "failed while reading CSV");
        rc = SQLITE_IOERR;
    }
    if (rc == SQLITE_DONE) {
        rc = SQLITE_OK;
    }
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    free(insert_sql);
    free(line);
    fclose(file);
    return rc;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

type sqliteBackend struct {
	db       *C.sqlite3
	beginSQL string
}

func newSQLiteBackend(path string) (*sqliteBackend, error) {
	return newSQLiteBackendWithBegin(path, "immediate")
}

func newSQLiteBackendWithBegin(path, beginMode string) (*sqliteBackend, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var db *C.sqlite3
	var message *C.char
	if rc := C.open_sqlite(cPath, &db, &message); rc != C.SQLITE_OK {
		return nil, sqliteError(message, int(rc))
	}
	beginSQL := "BEGIN;"
	if beginMode == "immediate" {
		beginSQL = "BEGIN IMMEDIATE;"
	}
	b := &sqliteBackend{db: db, beginSQL: beginSQL}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA busy_timeout=30000;",
	} {
		if _, err := b.exec(pragma); err != nil {
			b.close()
			return nil, err
		}
	}
	return b, nil
}

func sqliteError(message *C.char, rc int) error {
	defer func() {
		if message != nil {
			C.free_sqlite_string(message)
		}
	}()
	if message == nil {
		return fmt.Errorf("sqlite error (%d)", rc)
	}
	return errors.New(C.GoString(message))
}

func (b *sqliteBackend) exec(sql string) (string, error) {
	if b == nil || b.db == nil {
		return "", errors.New("sqlite connection is closed")
	}
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	var output *C.char
	var message *C.char
	rc := C.exec_collect(b.db, cSQL, &output, &message)
	if rc != C.SQLITE_OK {
		return "", sqliteError(message, int(rc))
	}
	if output == nil {
		return "", nil
	}
	text := C.GoString(output)
	C.free(unsafe.Pointer(output))
	return text, nil
}

func (b *sqliteBackend) begin() error {
	_, err := b.exec(b.beginSQL)
	return err
}

func (b *sqliteBackend) commit() error {
	_, err := b.exec("COMMIT;")
	return err
}

func (b *sqliteBackend) rollback() {
	_, _ = b.exec("ROLLBACK;")
}

func (b *sqliteBackend) close() {
	if b != nil && b.db != nil {
		C.sqlite3_close(b.db)
		b.db = nil
	}
}

func importCSVToSQLite(dbPath, dataDir, schemaDir string) error {
	b, err := newSQLiteBackend(dbPath)
	if err != nil {
		return err
	}
	defer b.close()
	if err := b.execFile(filepath.Join(schemaDir, "sqlite_schema.sql")); err != nil {
		return err
	}
	if err := b.begin(); err != nil {
		return err
	}
	for _, table := range tpccTables {
		cTable := C.CString(table)
		cPath := C.CString(filepath.Join(dataDir, table+".csv"))
		var message *C.char
		rc := C.import_csv(b.db, cTable, cPath, &message)
		C.free(unsafe.Pointer(cTable))
		C.free(unsafe.Pointer(cPath))
		if rc != C.SQLITE_OK {
			b.rollback()
			return sqliteError(message, int(rc))
		}
	}
	if err := b.commit(); err != nil {
		b.rollback()
		return err
	}
	return b.execFile(filepath.Join(schemaDir, "sqlite_indexes.sql"))
}

func (b *sqliteBackend) execFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, statement := range strings.Split(string(data), ";") {
		if sql := strings.TrimSpace(statement); sql != "" {
			if _, err := b.exec(sql + ";"); err != nil {
				return err
			}
		}
	}
	return nil
}
