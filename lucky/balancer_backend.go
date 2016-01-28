package lucky

import (
	"encoding/base64"
	"errors"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pebbe/zmq4"
	"github.com/satori/go.uuid"
)

type BalancerBackendRequest struct {
	startTime time.Time
	reqId     string
	route     [][]byte
	body      []byte
}

type BalancerBackendReply struct {
	reqId string
	body  []byte
}

type BalancerBackendHeartbeat struct{}
type BalancerBackendClose struct{}

type BalancerBackend struct {
	id                string
	balancer          *Balancer
	heartbeatSent     time.Time
	heartbeatReceived time.Time
	requests          map[string]*BalancerBackendRequest
	incoming          chan interface{}
	outgoing          *zmq4.Socket
	state             int
	logger            *log.Entry
}

const BALANCER_BACKEND_BUFFER = 100

func NewBalancerBackend(id string, balancer *Balancer) (*BalancerBackend, error) {
	socket, err := balancer.system.zmq.NewSocket(zmq4.PUSH)
	if err != nil {
		return nil, err
	}
	err = socket.Connect(balancer.InternalZmqEndpoint())
	if err != nil {
		return nil, err
	}
	now := time.Now()
	backend := &BalancerBackend{
		id: id,
		logger: log.WithFields(log.Fields{
			"balancer": balancer.name,
			"backend":  base64.StdEncoding.EncodeToString([]byte(id)),
		}),
		heartbeatSent:     now,
		heartbeatReceived: now,
		requests:          make(map[string]*BalancerBackendRequest),
		balancer:          balancer,
		state:             BALANCER_BACKEND_ONLINE,
		incoming:          make(chan interface{}, BALANCER_BACKEND_BUFFER),
		outgoing:          socket,
	}
	backend.logger.Info("New backend")
	go backend.runIncoming()
	return backend, nil
}

func (self *BalancerBackend) AddRequest(route [][]byte, body []byte) error {
	reqId := uuid.NewV4().String()
	v := &BalancerBackendRequest{
		reqId:     reqId,
		startTime: time.Now(),
		route:     route,
		body:      body,
	}
	self.incoming <- v
	return nil
}

func (self *BalancerBackend) AddReply(reqId string, body []byte) error {
	v := &BalancerBackendReply{
		reqId: reqId,
		body:  body,
	}
	self.incoming <- v
	return nil
}

func (self *BalancerBackend) AddHeartbeat() error {
	v := &BalancerBackendHeartbeat{}
	self.incoming <- v
	return nil
}

func (self *BalancerBackend) AddClose() error {
	v := &BalancerBackendClose{}
	self.incoming <- v
	return nil
}

func (self *BalancerBackend) SendHeartbeat() error {
	self.logger.Debug("Send heartbeat")
	_, err := self.outgoing.SendMessage("BACK_PROXY", self.id, "HEARTBEAT")
	self.heartbeatSent = time.Now()
	return err
}

func (self *BalancerBackend) SendDisconnect() error {
	self.logger.Info("Backend disconnected")
	for _, req := range self.requests {
		_, err := self.outgoing.SendMessage("FRONT_PROXY", req.route, "", "")
		if err != nil {
			self.logger.WithField("error", err).Error("In balancer backend")
		}
	}
	self.state = BALANCER_BACKEND_OFFLINE
	_, err := self.outgoing.SendMessage("DISCONNECT", self.id)
	return err
}

func (self *BalancerBackend) handleClose(_ *BalancerBackendClose) error {
	close(self.incoming)
	return self.outgoing.Close()
}

func (self *BalancerBackend) handleRequest(req *BalancerBackendRequest) error {
	if self.state == BALANCER_BACKEND_OFFLINE {
		_, err := self.outgoing.SendMessage("IM_OFFLINE", req.route, "", req.body)
		return err
	}
	self.requests[req.reqId] = req
	_, err := self.outgoing.SendMessage("BACK_PROXY", self.id, "REQUEST", req.reqId, req.body)
	if err != nil {
		return err
	}
	self.heartbeatSent = time.Now()
	return nil
}

func (self *BalancerBackend) handleReply(reply *BalancerBackendReply) error {
	self.logger.WithFields(log.Fields{
		"request": reply.reqId,
		"payload": reply.body}).Debug("Request reply")
	req, pst := self.requests[reply.reqId]
	if !pst {
		return errors.New("Unknown request")
	}
	requestsHistogram.Observe(time.Since(req.startTime).Seconds() / 1000)
	delete(self.requests, reply.reqId)
	self.heartbeatReceived = time.Now()
	_, err := self.outgoing.SendMessage("FRONT_PROXY", req.route, "", reply.body)
	return err
}

func (self *BalancerBackend) handleHeartbeat(_ *BalancerBackendHeartbeat) error {
	self.logger.Debug("Heartbeat received")
	self.heartbeatReceived = time.Now()
	return nil
}

func (self *BalancerBackend) tick(data interface{}) error {
	switch t := data.(type) {
	default:
		return errors.New("Unknown data")
	case *BalancerBackendRequest:
		return self.handleRequest(t)
	case *BalancerBackendReply:
		return self.handleReply(t)
	case *BalancerBackendHeartbeat:
		return self.handleHeartbeat(t)
	case *BalancerBackendClose:
		return self.handleClose(t)
	}
}

func (self *BalancerBackend) runIncoming() {
	heartbeatTicker := time.After(100 * time.Millisecond)
	for {
		select {
		case data, more := <-self.incoming:
			if more {
				err := self.tick(data)
				if err != nil {
					self.logger.WithError(err).Error("Error in backend")
				}
			} else {
				return
			}
		case <-heartbeatTicker:
			self.heartbeaterTick()
			heartbeatTicker = time.After(100 * time.Millisecond)
		}
	}
}

func (self *BalancerBackend) heartbeaterTick() {
	if time.Since(self.heartbeatSent).Seconds() > 3 {
		err := self.SendHeartbeat()
		if err != nil {
			self.logger.WithError(err).Error("Error in backend")
		}
	}

	if time.Since(self.heartbeatReceived).Seconds() > 10 {
		err := self.SendDisconnect()
		if err != nil {
			self.logger.WithError(err).Error("Error in backend")
		}
		return
	}
}
