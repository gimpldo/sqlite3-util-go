package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	sqlite3 "github.com/gimpldo/go-sqlite3"
	"github.com/gimpldo/sqlite3-util-go/sqlite3tracemask"
)

func traceCallback(info sqlite3.TraceInfo) int {
	// Not very readable but may be useful; uncomment next line in case of doubt:
	//fmt.Printf("Trace: %#v\n", info)

	var dbErrText string
	if info.DBError.Code != 0 || info.DBError.ExtendedCode != 0 {
		dbErrText = fmt.Sprintf("; DB error: %#v", info.DBError)
	} else {
		dbErrText = "."
	}

	// Show the Statement-or-Trigger text in curly braces ('{', '}')
	// since from the *paired* ASCII characters they are
	// the least used in SQL syntax, therefore better visual delimiters.
	// Maybe show 'ExpandedSQL' the same way as 'StmtOrTrigger'.
	//
	// A known use of curly braces (outside strings) is
	// for ODBC escape sequences. Not likely to appear here.
	//
	// Template languages, etc. don't matter, we should see their *result*
	// at *this* level.
	// Strange curly braces in SQL code that reached the database driver
	// suggest that there is a bug in the application.
	// The braces are likely to be either template syntax or
	// a programming language's string interpolation syntax.

	var expandedText string
	if info.ExpandedSQL != "" {
		if info.ExpandedSQL == info.StmtOrTrigger {
			expandedText = " no change when expanded"
		} else {
			expandedText = fmt.Sprintf(" expanded {%q}", info.ExpandedSQL)
		}
	} else {
		expandedText = ""
	}

	fmt.Printf("Trace: ev 0x%x, conn 0x%x, stmt 0x%x {%q}%s; %d ns%s\n",
		info.EventCode, info.ConnHandle, info.StmtHandle,
		info.StmtOrTrigger, expandedText,
		info.RunTimeNanosec,
		dbErrText)
	return 0
}

var nRows int     // Number of Rows to generate (for *each* approach tested)
var rowSeqNum int // Row Sequence Number

var noteTextPattern string

var rollbackAlways bool // Rollback (abort) transactions instead of committing

// txWrap is a kind of "Transaction Wrapper", very specialized (and simplified)
// for this test's needs:
//
// Usually the function that does the work (txFunc) should return error
// and the transaction wrapper should pass that error back to its caller,
// but here we use 'log.Panic...' since it simplifies test code
// and still allows cleanup to work.
//
// Based on idea posted by user 'user7610' (January 18, 2016) on Stack Overflow:
// http://stackoverflow.com/a/34851179   (see the long URL below)
// http://stackoverflow.com/questions/34842322/can-i-recover-from-panic-handle-the-error-then-panic-again-and-keep-the-origin/34851179#34851179
//
func txWrap(db *sql.DB, txFunc func(*sql.Tx)) (err error) {
	panicked := true

	tx, txErr := db.Begin()
	if txErr != nil {
		return txErr
		//TODO: return errors.Wrap(err, "Begin transaction failed in wrapper")
		// but it's not available in go1.6
	}

	defer func() {
		// DO NOT call recover(), check the flag ('panicked') instead
		// because after recover() neither rethrowing
		// nor accessing the stack trace (of received panic) is possible.
		// A new panic() would be unrelated to the old one.
		if panicked {
			tx.Rollback()
			return
		}

		if rollbackAlways { // unusual handling introduced for testing purpose
			tx.Rollback()
		} else { // the reasonable thing to do: commit if all went OK
			err = tx.Commit()
		}
	}()

	txFunc(tx)

	panicked = false // we know for sure, since we reached the end

	return nil // no error returned does *not* necessarily mean success
	// in *this* case: remember that txFunc reports problems with panic()
}

// TxWrapErr (draft) is a kind of "Transaction Wrapper";
// not used or needed here (just saved the work in progress).
//
// Based on idea posted by user 'Luke' (May 6, 2014) on Stack Overflow:
// http://stackoverflow.com/a/23502629   (see the long URL below)
// http://stackoverflow.com/questions/16184238/database-sql-tx-detecting-commit-or-rollback/23502629#23502629
//
// TODO: pass an interface instead of *sql.Tx
//        to prevent unwanted calls to Commit() or Rollback();
//        may also help with the lack of common interface covering DB and Tx.
//
func TxWrapErr(db *sql.DB, txFunc func(*sql.Tx) error) (err error) {
	tx, txErr := db.Begin()
	if txErr != nil {
		return txErr
	}

	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%s", p)
			}
		}
		if err != nil {
			tx.Rollback()
			return
		}

		if rollbackAlways { // unusual handling introduced for testing purpose
			tx.Rollback()
		} else { // the reasonable thing to do: commit if all went OK
			err = tx.Commit()
		}
	}()

	return txFunc(tx)
}

func main() {
	var dbFilename string
	var maskStr string
	var maskConf sqlite3tracemask.Config

	sqlite3tracemask.PrepareBoolArgsParsing(&maskConf)
	sqlite3tracemask.PrepareStringArgParsing(&maskStr)

	flag.StringVar(&dbFilename, "db", "", "SQLite database filename")
	flag.StringVar(&noteTextPattern, "search-pat", "", "Search pattern for SELECT")
	flag.IntVar(&nRows, "nrows", 4, "Number of rows to generate (for each approach tested)")
	flag.BoolVar(&rollbackAlways, "rollback", false,
		"Rollback (abort) transactions instead of committing")

	flag.Parse()

	sqlite3tracemask.DecodeStringArg(&maskConf, maskStr)

	// The event mask will be the union of boolean flags and string flag.
	// This is for illustration purpose. Probably you should use
	// either the boolean flags or the string flag (short form), not both.

	fmt.Printf("Short form of mask: {%s}\n", maskConf.GenerateStringArg())
	fmt.Printf("Long form of mask (separate flags): {%s}\n", maskConf.GenerateBoolArgs())
	fmt.Printf("Numeric mask: 0x%x\n", maskConf.EventMask())

	sql.Register("sqlite3_tracing",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.SetTrace(&sqlite3.TraceConfig{
					Callback:        traceCallback,
					EventMask:       maskConf.EventMask(),
					WantExpandedSQL: true,
				})
				return err
			},
		})

	if dbFilename == "" {
		fmt.Println("SQLite database filename not specified. Use --db=...")
		os.Exit(3)
	}

	os.Exit(dbMain(dbFilename))
}

// Harder to do DB work in main().
// It's better with a separate function because
// 'defer' and 'os.Exit' don't go well together.
//
// DO NOT use 'log.Fatal...' below: remember that it's equivalent to
// Print() followed by a call to os.Exit(1) --- and
// we want to avoid Exit() so 'defer' can do cleanup.
// Use 'log.Panic...' instead.

func dbMain(filename string) int {
	db, err := sql.Open("sqlite3_tracing", filename)
	if err != nil {
		fmt.Printf("Failed to open database '%s': %#+v\n",
			filename, err)
		return 1
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Panic(err)
	}

	dbSetup(db)

	dbDoInsert(db)

	err = txWrap(db, txDoInsert)
	if err != nil {
		log.Panicf("Transaction Wrapper error: %v\n", err)
	}

	dbDoInsertPrepared(db)

	err = txWrap(db, txDoInsertPrepared)
	if err != nil {
		log.Panicf("Transaction Wrapper error: %v\n", err)
	}

	dbDoSelect(db)

	err = txWrap(db, txDoSelect)
	if err != nil {
		log.Panicf("Transaction Wrapper error: %v\n", err)
	}

	dbDoSelectPrepared(db)

	err = txWrap(db, txDoSelectPrepared)
	if err != nil {
		log.Panicf("Transaction Wrapper error: %v\n", err)
	}

	return 0
}

// 'DDL' stands for "Data Definition Language":

// Note: "INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT" causes the error
// 'near "AUTOINCREMENT": syntax error'; without "NOT NULL" it works.
const tableDDL = `CREATE TABLE t1 (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 seq_num INTEGER NOT NULL,
 note VARCHAR NOT NULL
)`

// 'DML' stands for "Data Manipulation Language":

const insertDML = "INSERT INTO t1 (seq_num, note) VALUES (?, ?)"
const selectDML = "SELECT id, seq_num, note FROM t1 WHERE note LIKE ?"

const noteTextPrefix = "bla-1234567890"

func dbSetup(db *sql.DB) {
	var err error

	_, err = db.Exec("DROP TABLE IF EXISTS t1")
	if err != nil {
		log.Panic(err)
	}
	_, err = db.Exec(tableDDL)
	if err != nil {
		log.Panic(err)
	}
}

func dbDoInsert(db *sql.DB) {
	const Descr = "DB-imm"
	for i := 0; i < nRows; i++ {
		result, err := db.Exec(insertDML, rowSeqNum, noteTextPrefix+Descr)
		if err != nil {
			log.Panic(err)
		}

		resultDoCheck(result, Descr, i)

		rowSeqNum++
	}
}

func txDoInsert(tx *sql.Tx) {
	const Descr = "Tx-imm"
	for i := 0; i < nRows; i++ {
		result, err := tx.Exec(insertDML, rowSeqNum, noteTextPrefix+Descr)
		if err != nil {
			log.Panic(err)
		}

		resultDoCheck(result, Descr, i)

		rowSeqNum++
	}
}

func dbDoInsertPrepared(db *sql.DB) {
	const Descr = "DB-Prepare"

	stmt, err := db.Prepare(insertDML)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	for i := 0; i < nRows; i++ {
		result, err := stmt.Exec(rowSeqNum, noteTextPrefix+Descr)
		if err != nil {
			log.Panic(err)
		}

		resultDoCheck(result, Descr, i)

		rowSeqNum++
	}
}

func txDoInsertPrepared(tx *sql.Tx) {
	const Descr = "Tx-Prepare"

	stmt, err := tx.Prepare(insertDML)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	for i := 0; i < nRows; i++ {
		result, err := stmt.Exec(rowSeqNum, noteTextPrefix+Descr)
		if err != nil {
			log.Panic(err)
		}

		resultDoCheck(result, Descr, i)

		rowSeqNum++
	}
}

func resultDoCheck(result sql.Result, callerDescr string, callIndex int) {
	lastID, err := result.LastInsertId()
	if err != nil {
		log.Panic(err)
	}
	nAffected, err := result.RowsAffected()
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Exec result for %s (%d): ID = %d, affected = %d\n", callerDescr, callIndex, lastID, nAffected)
}

func dbDoSelect(db *sql.DB) {
	rows, err := db.Query(selectDML, noteTextPattern)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	rowsDoFetch(rows)
}

func txDoSelect(tx *sql.Tx) {
	rows, err := tx.Query(selectDML, noteTextPattern)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	rowsDoFetch(rows)
}

func dbDoSelectPrepared(db *sql.DB) {
	stmt, err := db.Prepare(selectDML)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(noteTextPattern)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	rowsDoFetch(rows)
}

func txDoSelectPrepared(tx *sql.Tx) {
	stmt, err := tx.Prepare(selectDML)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(noteTextPattern)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	rowsDoFetch(rows)
}

func rowsDoFetch(rows *sql.Rows) {
	for rows.Next() {
		// ...
	}
	if err := rows.Err(); err != nil {
		log.Panic(err)
	}
}

func txDoX(tx *sql.Tx) {

}

func dbDoX(db *sql.DB) {

}
