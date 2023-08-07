package main

import (
	"authexample/controllers"
	"flag"
	"fmt"
	"net/http"
)

func main() {
	fmt.Println("API Auth Example")
	var port string
	flag.StringVar(&port, "port", "3000", "Specifies Application Port")
	flag.Parse()

	fmt.Println("Starting Webservice with", "port", port)

	http.ListenAndServe(":"+port, controllers.RegisterRoutes())

}
