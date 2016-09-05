package sqlite3tracemask

import (
	"flag"
	"strings"

	sqlite3 "github.com/gimpldo/go-sqlite3"
)

type Config struct {
	Stmt    bool
	Profile bool
	Row     bool
	Close   bool
}

const (
	stmtArg    = "trace-stmt"
	profileArg = "trace-profile"
	rowArg     = "trace-row"
	closeArg   = "trace-close"
)

func PrepareBoolArgsParsing(dest *Config) {
	// Usage messages (last argument of 'flag.BoolVar') are based on
	// SQLite 3.14 documentation (as of September 2, 2016)
	// for SQL Trace Hook = sqlite3_trace_v2():
	flag.BoolVar(&dest.Stmt, stmtArg, false,
		"Event: statement first begins running, possibly the start of each trigger subprogram")
	flag.BoolVar(&dest.Profile, profileArg, false,
		"Event: statement finishes, gives estimated number of nanoseconds it took to run")
	flag.BoolVar(&dest.Row, rowArg, false,
		"Event: a statement generates a single row of result")
	flag.BoolVar(&dest.Close, closeArg, false,
		"Event: database connection closes")
}

func PrepareStringArgParsing(dest *string) {
	flag.StringVar(dest, "trace-mask", "",
		"Supported SQLite trace event codes: s=Stmt, p=Profile, r=Row, c=Close")
}

func DecodeStringArg(dest *Config, s string) {
	for _, c := range s {
		switch c {
		case 's':
			dest.Stmt = true
		case 'p':
			dest.Profile = true
		case 'r':
			dest.Row = true
		case 'c':
			dest.Close = true
		}
	}
}

func (c *Config) GenerateStringArg() string {
	sf := []string{} // 'sf' stands for "String Fragments"
	if c.Stmt {
		sf = append(sf, "s")
	}
	if c.Profile {
		sf = append(sf, "p")
	}
	if c.Row {
		sf = append(sf, "r")
	}
	if c.Close {
		sf = append(sf, "c")
	}
	return strings.Join(sf, "")
}

func (c *Config) GenerateBoolArgs() string {
	sf := []string{} // 'sf' stands for "String Fragments"
	if c.Stmt {
		sf = append(sf, stmtArg)
	}
	if c.Profile {
		sf = append(sf, profileArg)
	}
	if c.Row {
		sf = append(sf, rowArg)
	}
	if c.Close {
		sf = append(sf, closeArg)
	}
	return "--" + strings.Join(sf, " --")
}

func (c *Config) EventMask() uint {
	var mask uint
	if c.Stmt {
		mask |= sqlite3.TraceStmt
	}
	if c.Profile {
		mask |= sqlite3.TraceProfile
	}
	if c.Row {
		mask |= sqlite3.TraceRow
	}
	if c.Close {
		mask |= sqlite3.TraceClose
	}
	return mask
}
