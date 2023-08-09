package main

import (
	"authexample/controllers"
	"authexample/db"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("API Auth Example")
	var port string
	flag.StringVar(&port, "port", "3000", "Specifies Application Port")
	flag.Parse()

	dbSErr := db.Init()
	if dbSErr == nil {

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		go startServer(port)

		<-signals

		dbCErr := db.Close()

		if dbCErr != nil {
			fmt.Println(dbCErr.Error())
		}

	} else {
		fmt.Println(dbSErr.Error())
	}
}

func startServer(port string) {
	fmt.Println("Starting Webservice with", "port", port)
	http.ListenAndServe(":"+port, controllers.RegisterRoutes())
}
