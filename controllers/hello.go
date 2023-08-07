package controllers

import (
	"authexample/shared"
	"net/http"
)

type HelloController struct {
}

func HelloWorldHandler(writer http.ResponseWriter, request *http.Request) {
	shared.EncodeResponse("Hello World!", writer)
}
