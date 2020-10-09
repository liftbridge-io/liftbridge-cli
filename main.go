package main

import (
	"log"
	"main/cli"
	"os"
)

func main() {
	err := cli.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
