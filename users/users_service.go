package users

import (
	"authexample/db"
	"authexample/shared"
	"errors"
	"fmt"
	"time"
)

func GetAllUsers() []User {
	var users []User
	result, err := db.Query(GET_ALL_USERS)
	if err == nil {
		if result != nil {
			_user := User{}
			if len(*result) > 0 {
				for _, row := range *result {
					_user.USER_ID = int(row[0].(int64))
					_user.USERNAME = row[1].(string)
					_user.FULL_NAME = row[2].(string)
					_user.PASSWORD = row[3].(string)
					_user.EMAIL = row[4].(string)
					_user.CREATED_ON = row[5].(time.Time)
					_user.LAST_LOGIN = row[6].(time.Time)
					users = append(users, _user)
				}
			}
		}
	}
	return users
}

func GetUser(username string) (User, error) {
	result, err := db.Query(GET_USER_BY_USERNAME, username)
	if err == nil {
		if result != nil {
			_user := User{}
			if len(*result) > 0 {
				for _, row := range *result {
					_user.USER_ID = int(row[0].(int64))
					_user.USERNAME = row[1].(string)
					_user.FULL_NAME = row[2].(string)
					_user.PASSWORD = row[3].(string)
					_user.EMAIL = row[4].(string)
					_user.CREATED_ON = row[5].(time.Time)
					_user.LAST_LOGIN = row[6].(time.Time)
				}
				return _user, nil
			}
		}
		return User{}, fmt.Errorf("User %s not found", username)
	}
	return User{}, err
}

func DeleteUser(username string) (User, error) {
	user, err := GetUser(username)
	if err == nil {
		return user, db.Execute(DELETE_USER_BY_USERNAME, user.USERNAME)
	}
	return user, err
}

func (u *User) AddUser() (User, error) {
	// ... validation
	if u.USER_ID <= 0 || len(u.USERNAME) <= 0 || len(u.FULL_NAME) <= 0 || len(u.PASSWORD) <= 0 {
		return User{}, errors.New("check the following fields 'USER_ID', 'USERNAME', 'FULL_NAME' or 'PASSWORD'")
	}
	pw, _ := shared.HashPassword(u.PASSWORD)

	_, err := db.Query(ADD_USER, u.USER_ID, u.USERNAME, u.FULL_NAME, pw, u.EMAIL)
	if err == nil {
		return GetUser(u.USERNAME)
	} else {
		return User{}, err
	}
}

func AuthenticateUser(username, input string) error {
	// .. get user
	user, err := GetUser(username)
	if err == nil {
		compErr := shared.ComparePassword(user.PASSWORD, input)
		if compErr == nil {
			return compErr
		} else {
			err = errors.New("username or password invalid")
		}
	}
	return err
}
