package effectus_test

import "database/sql"

func openRaw(path string) (*sql.DB, error) { return sql.Open("sqlite", path) }
