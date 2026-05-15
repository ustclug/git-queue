package main

import (
	"log"
	"os"

	"github.com/ustclug/git-queue/cmd"
)

func main() {
	if _, ok := os.LookupEnv("INVOCATION_ID"); ok {
		// invoked by systemd
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}

	_ = cmd.Root().Execute()
}
