package users

import "time"

// User struct for holding data
type User struct {
	USER_ID    int
	USERNAME   string
	FULL_NAME  string
	PASSWORD   string
	EMAIL      string
	CREATED_ON time.Time
	LAST_LOGIN time.Time
}
