package database

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func NewDatabaseConnection() *sql.DB {
	db, err := sql.Open("sqlite3", "./database.db")
	if err != nil {
		panic("error on connection with database")
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY, name VARCHAR(256) NULL);")
	if err != nil {
		panic("error on creating users table")
	}

	return db
}
