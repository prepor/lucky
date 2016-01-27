package lucky

import (
	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	"github.com/pebbe/zmq4"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type System struct {
	zmq       *zmq4.Context
	Running   atomic.Value
	processes sync.WaitGroup
	config    *Config
}

func (sys *System) Start() {
	go httpServer(sys)
	log.Debug("Start with config:")
	log.Debug(spew.Sdump(sys.config))
	for _, config := range sys.config.Balancers {
		_, err := sys.StartBalancer(config.Name, config.Front, config.Back)
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"config": config,
			}).Fatal("Can't start balancer")
			os.Exit(1)
		}
	}

	sys.processes.Wait()
	sys.zmq.Term()
}

func NewSystem(config *Config) (*System, error) {
	ctx, err := zmq4.NewContext()
	if err != nil {
		panic("Can't create ZMQ context")
	}
	var running atomic.Value
	running.Store(true)
	var processes sync.WaitGroup
	return &System{
		zmq:       ctx,
		Running:   running,
		config:    config,
		processes: processes,
	}, nil
}

func (self *System) CreateSocket(config *SocketConfig) (*zmq4.Socket, error) {
	socket, err := self.zmq.NewSocket(zmq4.ROUTER)
	if err != nil {
		return nil, err
	}
	for _, bind := range config.Bind {
		err = socket.Bind(bind)
		if err != nil {
			return nil, err
		}
	}
	err = socket.SetLinger(100 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	return socket, nil
}

func (s *System) StartBalancer(name string, front_config *SocketConfig, back_config *SocketConfig) (*Balancer, error) {
	front, err := s.CreateSocket(front_config)
	if err != nil {
		return nil, err
	}
	back, err := s.CreateSocket(back_config)
	if err != nil {
		return nil, err
	}
	balancer := &Balancer{
		name:    name,
		front:   front,
		back:    back,
		workers: make(map[string]*Worker),
		system:  s,
	}
	s.processes.Add(1)
	go func() {
		defer front.Close()
		defer back.Close()
		defer s.processes.Done()
		log.WithFields(log.Fields{
			"name":         name,
			"front_config": front_config,
			"back_config":  back_config,
		}).Info("Start zmq balancer")
		balancer.Run()
	}()
	return balancer, nil
}
