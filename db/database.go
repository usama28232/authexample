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

func Init() error {
	psqlconn := fmt.Sprintf("Database host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	fmt.Println(psqlconn)
	var err error
	// open database
	if db != nil {
		return nil
	}
	mutex.Lock()
	defer mutex.Unlock()
	db, err = sql.Open("postgres", psqlconn)
	checkError(err)

	return err
}

func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}

func getConnection() (*sql.DB, error) {
	return db, nil
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func Execute(query string, args ...any) error {
	conn, connErr := getConnection()
	if connErr == nil {
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
