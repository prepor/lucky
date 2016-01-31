package lucky

import (
	"os"

	"gopkg.in/edn.v1"

	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/prepor/lucky/src/util"
)

type System struct {
	processes    *sync.WaitGroup
	config       *Config
	commands     chan interface{}
	commandsMult *util.Mult
}

func (sys *System) Start() {
	backends := make(map[edn.Keyword]*Backend)

	for name, config := range sys.config.Backends {
		backend, err := NewBackend(sys, string(name), config)
		if err != nil {
			log.WithError(err).Fatal("Can't start backend")
			os.Exit(1)
		}
		backends[name] = backend
	}

	for _, config := range sys.config.Frontends {
		switch string(config.Type) {
		case "zmq":
			backend, pst := backends[config.Backend]
			if !pst {
				log.Fatalf("There are no backend %s", config.Backend)
				os.Exit(1)
			}
			_, err := NewZMQFrontend(sys, config, backend)
			if err != nil {
				log.WithError(err).Fatal("Can't start frontend")
				os.Exit(1)
			}
		case "http":
			NewHttpFrontend(sys, config.Endpoint)
		}
	}

	sys.processes.Wait()
}

func NewSystem(config *Config) (*System, error) {
	commands := make(chan interface{}, 10)
	return &System{
		config:       config,
		processes:    new(sync.WaitGroup),
		commands:     commands,
		commandsMult: util.NewMult(commands),
	}, nil
}

func (self *System) Stop() {
	defer func() {
		recover()
		return
	}()
	close(self.commands)
}
