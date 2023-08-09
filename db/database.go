package db

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
)

var (
	db    *sql.DB = nil
	mutex sync.Mutex
)

func init_connection() error {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	var err error
	// open database
	if db != nil {
		return nil
	}
	mutex.Lock()
	db, err = sql.Open("postgres", psqlconn)
	defer mutex.Unlock()
	checkError(err)

	return err
}

func getConnection() (*sql.DB, error) {
	var error error = nil
	if db == nil {
		error = init_connection()
	}
	return db, error
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func Execute(query string, args ...any) error {
	conn, connErr := getConnection()
	if connErr == nil {
		defer conn.Close()
		var err error
		if len(args) > 0 {
			_, err = conn.Exec(query, args...)

		} else {
			_, err = conn.Exec(query)
		}
		return err
	}
	return connErr
}

func Query(query string, args ...any) (*([][]any), error) {
	conn, connErr := getConnection()
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Close()
	var rows *sql.Rows
	var err error
	if len(args) > 0 {
		rows, err = conn.Query(query, args...)

	} else {
		rows, err = conn.Query(query)
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
