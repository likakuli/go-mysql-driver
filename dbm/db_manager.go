package dbm

import (
	"database/sql"
	"errors"

	_ "github.com/go-sql-driver/mysql"
)

type MySqlDBManager struct {
	DB *sql.DB
}

func (m *MySqlDBManager) Initialize(connString string) error {
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return errors.New("failed to open db via connection.")
	}
	err = db.Ping()
	if err != nil {
		return errors.New("invalid db conection")
	}
	m.DB = db
	return nil
}
