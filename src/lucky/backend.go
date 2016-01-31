package lucky

import (
	"errors"
	"net"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prepor/zmtp"
)

type Backend struct {
	name      string
	requests  chan *Request
	system    *System
	control   chan bool
	logger    *log.Entry
	config    *BackendConfig
	instances *sync.WaitGroup
}

func (self *Backend) AddRequest(req *Request) error {
	select {
	case self.requests <- req:
		return nil
	default:
		return errors.New("Can't send request. System overloaded")
	}
}

func NewBackend(system *System, name string, config *BackendConfig) (*Backend, error) {
	self := &Backend{
		system:    system,
		name:      name,
		logger:    log.WithField("backend", name),
		requests:  make(chan *Request, 100),
		instances: new(sync.WaitGroup),
		config:    config,
		control:   make(chan bool),
	}
	err := self.initListeners()
	if err != nil {
		return nil, err
	}
	go self.commandsListener()
	go self.startRejector()
	self.logger.Info("Start")
	return self, nil
}

func (self *Backend) commandsListener() {
	commands := make(chan interface{}, 10)
	err := self.system.commandsMult.Tap(commands, true)
	if err != nil {
		close(self.control)
		return
	}
	for range commands {
	}
	self.logger.Info("Stop")
	close(self.control)
}

func (self *Backend) initListeners() error {
	listeners := make([]*zmtp.Listener, 0, len(self.config.Bind))
	for _, endpoint := range self.config.Bind {
		listener, err := self.startListener(endpoint)
		if err != nil {
			for _, listener := range listeners {
				listener.Close()
			}
			return err
		}
		listeners = append(listeners, listener)
	}
	return nil
}

func (self *Backend) startListener(endpoint string) (*zmtp.Listener, error) {
	listener, err := zmtp.Listen(&zmtp.SocketConfig{
		Type:     zmtp.DEALER,
		Endpoint: endpoint,
	})
	if err != nil {
		return nil, err
	}
	self.system.processes.Add(1)
	go self.listenerLoop(listener)
	return listener, nil
}

func (self *Backend) startRejector() {
	for {
		self.instances.Wait()
		select {
		default:
			time.Sleep(100 * time.Millisecond)
		case <-self.control:
			return
		case v, ok := <-self.requests:
			if !ok {
				return
			}
			self.logger.Warn("No backends")
			v.Answer([][]byte{[]byte("")})
		}
	}
}

func (self *Backend) listenerLoop(listener *zmtp.Listener) {
	defer self.system.processes.Done()
	for {
		select {
		case <-self.control:
			listener.Close()
			return
		case v, ok := <-listener.Accept():
			if !ok {
				return
			}
			if v.Err != nil {
				self.logger.WithError(v.Err).Error("Can't accept socket")
			} else {
				self.instances.Add(1)
				self.system.processes.Add(1)
				go self.socketLoop(v.Socket, &v.Addr)
			}
		}
	}
}

func (self *Backend) socketLoop(socket *zmtp.Socket, addr *net.Addr) {
	logger := self.logger.WithField("remote", addr)
	requests := make(map[string]*Request)
	defer self.dropRequests(requests)
	defer socket.Close()
	defer logger.Info("Close")
	defer self.instances.Done()
	defer self.system.processes.Done()

	logger.Info("Connected")

	read := socket.Read()
	for {
		select {
		case <-self.control:
			return
		case req, ok := <-self.requests:
			if !ok {
				return
			}
			if err := socket.Send(req.Id, req.Route, "", req.Payload); err != nil {
				self.logger.WithError(err).Error("Can't send request")
				req.Answer([][]byte{[]byte("")})
			} else {
				logger.WithField("request", req.Id).Debug("New backend request")
				requests[req.Id] = req
			}
		case v, ok := <-read:
			if !ok {
				return
			}
			route, payload, err := MsgWihDelim(v)
			if err != nil {
				logger.Error("Can't parse message")
			} else if len(route) == 0 {
				logger.Error("Bad formed message")
			} else {
				reqId := string(route[0])
				req, pst := requests[reqId]
				if pst {
					delete(requests, reqId)
					logger.WithField("request", req.Id).Debug("Request reply")
					req.Answer(payload)
				} else {
					logger.WithField("request", reqId).Error("Can't find request")
				}
			}
		}
	}
}

func (self *Backend) dropRequests(requests map[string]*Request) {
	for _, req := range requests {
		req.Answer([][]byte{[]byte("")})
	}
}
