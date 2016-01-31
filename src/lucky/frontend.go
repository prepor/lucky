package lucky

import (
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/prepor/zmtp"
)

type ZMQFrontend struct {
	system  *System
	config  *FrontendConfig
	logger  *log.Entry
	backend *Backend
	control chan bool
}

func NewFrontend(system *System, config *FrontendConfig, initBackend *Backend) (*ZMQFrontend, error) {
	self := &ZMQFrontend{
		system:  system,
		logger:  log.WithField("frontend", config.Bind),
		config:  config,
		backend: initBackend,
		control: make(chan bool),
	}
	err := self.initListeners(initBackend)
	if err != nil {
		return nil, err
	}
	go self.commandsListener()
	self.logger.Info("Start")
	return self, nil
}

func (self *ZMQFrontend) commandsListener() {
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

func (self *ZMQFrontend) initListeners(initBackend *Backend) error {
	listeners := make([]*zmtp.Listener, 0, len(self.config.Bind))
	for _, endpoint := range self.config.Bind {
		listener, err := self.startListener(endpoint, initBackend)
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

func (self *ZMQFrontend) startListener(endpoint string, initBackend *Backend) (*zmtp.Listener, error) {
	listener, err := zmtp.Listen(&zmtp.SocketConfig{
		Type:     zmtp.ROUTER,
		Endpoint: endpoint,
	})
	if err != nil {
		return nil, err
	}
	self.system.processes.Add(1)
	go self.listenerLoop(listener, initBackend)
	return listener, nil
}

func (self *ZMQFrontend) listenerLoop(listener *zmtp.Listener, initBackend *Backend) {
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
				self.system.processes.Add(1)
				go self.socketLoop(v.Socket, &v.Addr, initBackend)
			}
		}
	}
}

func (self *ZMQFrontend) socketLoop(socket *zmtp.Socket, addr *net.Addr, initBackend *Backend) {
	logger := self.logger.WithField("remote", addr)
	defer socket.Close()
	defer logger.Debug("Close")
	defer self.system.processes.Done()

	logger.Debug("Connected")

	backend := initBackend
	read := socket.Read()

	answers := make(chan *Request, 10)

	for {
		select {
		case <-self.control:
			return
		case v, ok := <-read:
			if !ok {
				return
			}
			route, payload, err := MsgWihDelim(v)
			if err != nil {
				self.logger.Error("Can't parse message")
				socket.Send(route, "", "")
			} else {
				req := NewRequest(route, payload, answers)
				logger.WithField("request", req.Id).Debug("New frontend request")
				if err := backend.AddRequest(req); err != nil {
					logger.WithError(err).Error("Error in request sending")
					socket.Send(route, "", "")
				}
			}
		case v := <-answers:
			err := socket.Send(v.Route, "", v.Reply)
			if err != nil {
				self.logger.WithError(err).Error("Can't send reply")
			}
		}
	}
}
