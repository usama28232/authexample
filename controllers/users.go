package controllers

import (
	"authexample/users"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func GetAllUsersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(users.GetAllUsers())
}

func CreateUserHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var newUser users.User
	_ = json.NewDecoder(r.Body).Decode(&newUser)
	value, err := newUser.AddUser()
	if err == nil {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(value)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
	}
}

func GetUserHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	username := params["username"]
	_user, err := users.GetUser(username)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(_user)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(err.Error())
	}
}

func DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	username := params["username"]
	_user, err := users.DeleteUser(username)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(_user)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(err.Error())
	}
}
