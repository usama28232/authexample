package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

var db *sql.DB = nil

func init_connection() {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	var err error
	// open database
	db, err = sql.Open("postgres", psqlconn)
	checkError(err)

	// close database
	// defer db.Close()

	// check db
	err = db.Ping()
	checkError(err)
}

func getConnection() *sql.DB {
	if db == nil {
		init_connection()
	}
	return db
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func Execute(query string, args ...any) error {
	getConnection()
	var err error
	if len(args) > 0 {
		_, err = db.Exec(query, args...)

	} else {
		_, err = db.Query(query)
	}
	return err
}

func Query(query string, args ...any) (*([][]any), error) {
	getConnection()
	var rows *sql.Rows
	var err error
	if len(args) > 0 {
		rows, err = db.Query(query, args...)

	} else {
		rows, err = db.Query(query)
	}
	if err == nil {
		defer rows.Close()

		cols, _ := rows.Columns()
		data := [][]any{}
		for rows.Next() {
			columns, columnPointers := generateColumnPointers(cols)
			err := rows.Scan(columnPointers...)
			if err == nil {
				data = append(data, columns)
			}
		}

		return &data, nil
	} else {
		fmt.Println("Error in Query", err)
	}
	// db.Close()
	return nil, err
}

func generateColumnPointers(cols []string) ([]any, []any) {
	columnCount := len(cols)
	columns := make([]any, columnCount)
	columnPointers := make([]any, columnCount)
	for i := 0; i < columnCount; i++ {
		columnPointers[i] = &columns[i]
	}
	return columns, columnPointers
}
