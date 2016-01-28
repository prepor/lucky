package lucky

import (
	"errors"
	"fmt"
	"math/rand"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pebbe/zmq4"
)

const (
	BALANCER_BACKEND_ONLINE = iota
	BALANCER_BACKEND_OFFLINE
)

type Balancer struct {
	name     string
	system   *System
	backends map[string]*BalancerBackendRef
	front    *zmq4.Socket
	back     *zmq4.Socket
	internal *zmq4.Socket
	config   *BalancerConfig
}

type BalancerBackendRef struct {
	backend *BalancerBackend
	state   int
}

func NewBalancer(system *System, config *BalancerConfig) (*Balancer, error) {
	front, err := system.CreateSocket(config.Front)
	if err != nil {
		return nil, err
	}
	back, err := system.CreateSocket(config.Back)
	if err != nil {
		return nil, err
	}

	internal, err := system.zmq.NewSocket(zmq4.PULL)
	if err != nil {
		return nil, err
	}
	err = internal.Bind(fmt.Sprintf("inproc://balancer_internal_%s", config.Name))
	if err != nil {
		return nil, err
	}
	err = internal.SetLinger(0)
	if err != nil {
		return nil, err
	}

	balancer := &Balancer{
		name:     config.Name,
		front:    front,
		back:     back,
		internal: internal,
		backends: make(map[string]*BalancerBackendRef),
		system:   system,
		config:   config,
	}
	system.processes.Add(1)
	go balancer.run()
	return balancer, nil
}

func (self *Balancer) loadBalance() (*BalancerBackend, error) {
	if len(self.backends) == 0 {
		return nil, errors.New("No backends")
	} else {
		keys := make([]string, 0, len(self.backends))
		for k, w := range self.backends {
			if w.state == BALANCER_BACKEND_ONLINE {
				keys = append(keys, k)
			}
		}
		random_key := keys[rand.Intn(len(keys))]
		return self.backends[random_key].backend, nil
	}
}

func (self *Balancer) InternalZmqEndpoint() string {
	return fmt.Sprintf("inproc://balancer_internal_%s", self.name)
}

func (self *Balancer) frontRequest(msg [][]byte) (route [][]byte, body []byte, err error) {
	for i, part := range msg {
		if len(part) == 0 && len(msg) == i+2 {
			return msg[:i], msg[i+1], nil
		} else if len(part) == 0 {
			return nil, nil, errors.New("Invalid front msg")
		}
	}
	return nil, nil, errors.New("Invalid front msg")
}

func (self *Balancer) handleFront(msg [][]byte) error {
	route, body, err := self.frontRequest(msg)
	if err != nil {
		return err
	}
	backend, err := self.loadBalance()
	if err != nil {
		log.WithField("balancer", self.name).Warn("No backends")
		self.front.SendMessage(route, "", "")
		return nil
	}
	err = backend.AddRequest(route, body)
	if err != nil {
		self.front.SendMessage(route, "", "")
		return err
	}
	return nil
}

func (self *Balancer) handleBack(msg [][]byte) error {
	backendId := string(msg[0])
	command := string(msg[1])
	switch command {
	default:
		return errors.New("Unknown command")
	case "READY":
		backend, err := NewBalancerBackend(backendId, self)
		if err != nil {
			return err
		}
		self.backends[backendId] = &BalancerBackendRef{
			backend: backend,
			state:   BALANCER_BACKEND_ONLINE}
	case "HEARTBEAT":
		ref, pst := self.backends[backendId]
		if pst == false {
			return errors.New("Can't find worker")
		}
		return ref.backend.AddHeartbeat()
	case "REPLY":
		ref, pst := self.backends[backendId]
		if pst == false {
			return errors.New("Can't find worker")
		}
		return ref.backend.AddReply(string(msg[2]), msg[3])
	case "DISCONNECT":
		ref, pst := self.backends[backendId]
		if pst {
			ref.backend.AddClose()
			delete(self.backends, backendId)
		}
	}
	return nil
}

func (self *Balancer) handleInternal(msg [][]byte) error {
	command := string(msg[0])
	switch command {
	default:
		return errors.New("Unknown command")
	case "FRONT_PROXY":
		_, err := self.front.SendMessage(msg[1:])
		return err
	case "BACK_PROXY":
		_, err := self.back.SendMessage(msg[1:])
		return err
	case "IM_OFFLINE":
		self.handleFront(msg[1:])
	case "DISCONNECT":
		id := string(msg[1])
		ref, pst := self.backends[id]
		if pst {
			ref.backend.AddClose()
			delete(self.backends, id)
		}
		_, err := self.back.SendMessage(id, "DISCONNECT")
		return err
	}
	return nil
}

func (self *Balancer) handleMessages(socket *zmq4.Socket, fn func([][]byte) error) error {
	for {
		msg, err := socket.RecvMessageBytes(zmq4.DONTWAIT)
		if err == syscall.EAGAIN {
			return nil
		}
		if err != nil {
			return err
		} else {
			err = fn(msg)
		}
		return err
	}
}

var BALANCER_FORCE_STOP_TIMEOUT = 5 * time.Second

func (self *Balancer) Loop() {
	poller := zmq4.NewPoller()
	poller.Add(self.front, zmq4.POLLIN)
	poller.Add(self.back, zmq4.POLLIN)
	poller.Add(self.internal, zmq4.POLLIN)
	running := true
	force_stop := false
	var force_stopped_at time.Time
	for {
		if !self.system.Running.Load().(bool) && running {
			self.front.Close()
			poller = zmq4.NewPoller()
			poller.Add(self.back, zmq4.POLLIN)
			poller.Add(self.internal, zmq4.POLLIN)

			for k := range self.backends {
				_, err := self.back.SendMessage(k, "DISCONNECT")
				if err != nil {
					log.WithFields(log.Fields{
						"error":    err,
						"balancer": self.name,
					}).Error("Error while sending disconnect")
				}
			}
			running = false
			force_stop = true
			force_stopped_at = time.Now()
		}

		if force_stop && time.Since(force_stopped_at) > BALANCER_FORCE_STOP_TIMEOUT {
			log.WithField("balancer", self.name).Warn("Force stopping")
			for k, ref := range self.backends {
				ref.backend.AddClose()
				delete(self.backends, k)
			}
		}
		if !running && len(self.backends) == 0 {
			break
		}

		sockets, err := poller.Poll(100 * time.Millisecond)
		if err != nil {
			log.WithFields(log.Fields{
				"error":    err,
				"balancer": self.name,
			}).Error("Polling error")
		}
		for _, socket := range sockets {

			var err error
			var errSocket string
			switch socket.Socket {
			case self.front:
				err = self.handleMessages(self.front, self.handleFront)
				if err != nil {
					errSocket = "front"
				}
			case self.back:
				err = self.handleMessages(self.back, self.handleBack)
				if err != nil {
					errSocket = "back"
				}
			case self.internal:
				err = self.handleMessages(self.internal, self.handleInternal)
				if err != nil {
					errSocket = "internal"
				}
			}

			if err != nil {
				log.WithFields(log.Fields{
					"error":    err,
					"balancer": self.name,
				}).Errorf("Message handling error in %s socket", errSocket)
			}
		}
	}
	self.internal.Close()
}

func (self *Balancer) run() {
	defer self.front.Close()
	defer self.back.Close()
	defer self.system.processes.Done()
	log.WithFields(log.Fields{
		"name": self.config.Name,
	}).Info("Start zmq balancer")
	self.Loop()
}
