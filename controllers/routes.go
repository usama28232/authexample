package controllers

import (
	"authexample/shared"
	"authexample/users"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

var NOAUTH = []string{"/hello"}

func RegisterRoutes() *mux.Router {

	mux := mux.NewRouter()
	mux.Use(authMiddleware)

	mux.HandleFunc("/hello", HelloWorldHandler).Methods(http.MethodGet)

	mux.HandleFunc("/users", GetAllUsersHandler).Methods(http.MethodGet)
	mux.HandleFunc("/users", CreateUserHandler).Methods(http.MethodPost)
	mux.HandleFunc("/users/{username:[A-Za-z0-9]+}", GetUserHandler).Methods(http.MethodGet)
	mux.HandleFunc("/users/{username:[A-Za-z0-9]+}", DeleteUserHandler).Methods(http.MethodDelete)

	mux.StrictSlash(false)
	return mux
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if shared.CollectionContains(NOAUTH, r.URL.Path) {
			next.ServeHTTP(w, r)
		} else {
			authHeader := r.Header.Get("Authorization")

			if authHeader != "" && strings.HasPrefix(authHeader, "Basic ") {
				// Decode the base64-encoded username and password
				encodedCredentials := strings.TrimPrefix(authHeader, "Basic ")
				decoderStr := shared.DecodeBase64(encodedCredentials)
				if len(decoderStr) == 0 {
					http.Error(w, "Invalid Authorization header", http.StatusUnauthorized)
					return
				}

				cred := shared.StringSplit(decoderStr, ":")

				if len(cred) > 1 {
					authErr := users.AuthenticateUser(cred[0], cred[1])
					if authErr == nil {
						next.ServeHTTP(w, r)
					} else {
						http.Error(w, authErr.Error(), http.StatusUnauthorized)
						return
					}
				} else {
					http.Error(w, "Invalid Credentials", http.StatusUnauthorized)
					return
				}
			} else {
				// Authentication header is missing or invalid
				http.Error(w, "unable to parse Auth Header", http.StatusUnauthorized)
			}
		}

	})
}
