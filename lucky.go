package main

import (
	"flag"
	"fmt"
	"os"

	"os/signal"

	log "github.com/Sirupsen/logrus"
	"github.com/prepor/lucky/src/lucky"
)

import _ "net/http/pprof"

func signalsHandler(sys *lucky.System, signals chan os.Signal) {
	for {
		s := <-signals
		log.Infof("Signal received: %v", s)
		sys.Stop()
	}
}

func main() {
	verbose := flag.Bool("verbose", false, "Be verbose")
	flag.Usage = func() {
		fmt.Println("Usage: lucky CONFIG_PATH")
		flag.PrintDefaults()
	}
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("Path is required")
		flag.Usage()
		os.Exit(1)
	}

	// Logging
	if *verbose {
		log.SetLevel(log.DebugLevel)
	}

	config, err := lucky.ParseConfig(flag.Args()[0])
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Fatal("Error while parsing config")
	}
	sys, _ := lucky.NewSystem(config)
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, os.Kill)
	go signalsHandler(sys, signals)
	sys.Start()
}
