package lucky

import (
	"encoding/base64"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/pebbe/zmq4"
	"github.com/satori/go.uuid"
	"math/rand"
	"syscall"
	"time"
)

type Request struct {
	socket     *zmq4.Socket
	start_time time.Time
	request_id string
	route      [][]byte
}

const (
	ONLINE  = iota
	OFFLINE = iota
)

type state int

type Worker struct {
	id                string
	balancer          *Balancer
	heartbeatSent     time.Time
	heartbeatReceived time.Time
	reqId             uint64
	requests          map[string]*Request
	state             state
	logger            *log.Entry
}

type Balancer struct {
	name    string
	system  *System
	workers map[string]*Worker
	front   *zmq4.Socket
	back    *zmq4.Socket
}

func NewWorker(id string, balancer *Balancer) *Worker {
	now := time.Now()
	worker := &Worker{
		id: id,
		logger: log.WithFields(log.Fields{
			"balancer": balancer.name,
			"worker":   base64.StdEncoding.EncodeToString([]byte(id)),
		}),
		heartbeatSent:     now,
		heartbeatReceived: now,
		requests:          make(map[string]*Request),
		balancer:          balancer,
		reqId:             0,
		state:             ONLINE,
	}
	worker.logger.Info("New worker")
	return worker
}

func (self *Worker) SendRequest(front *zmq4.Socket, route [][]byte, body []byte) error {
	reqId := uuid.NewV4().String()
	req := &Request{
		socket:     front,
		request_id: reqId,
		start_time: time.Now(),
		route:      route,
	}
	self.requests[reqId] = req
	_, err := self.balancer.back.SendMessage(self.id, "REQUEST", reqId, body)
	if err != nil {
		return err
	}
	self.heartbeatSent = time.Now()
	return nil
}

func (self *Worker) SendHeartbeat() error {
	self.logger.Debug("Send heartbeat")
	_, err := self.balancer.back.SendMessage(self.id, "HEARTBEAT")
	self.heartbeatSent = time.Now()
	return err
}

func (self *Worker) HeartbeatReceived() {
	self.logger.Debug("Heartbeat received")
	self.heartbeatReceived = time.Now()
}

func (self *Worker) Reply(reqId string, payload []byte) error {
	self.logger.WithFields(log.Fields{
		"request": reqId,
		"payload": payload}).Debug("Request reply")
	req, pst := self.requests[reqId]
	if !pst {
		return errors.New("Unknown request")
	}
	requestsHistogram.Observe(time.Since(req.start_time).Seconds() / 1000)
	delete(self.requests, reqId)
	self.heartbeatReceived = time.Now()
	_, err := req.socket.SendMessage(req.route, "", payload)
	return err
}

func (self *Worker) Disconnect() {
	self.logger.Info("Worker disconnected")
	for _, req := range self.requests {
		req.socket.SendMessage(req.route, "", "")
	}
}

func frontRequest(msg [][]byte) (route [][]byte, body []byte, err error) {
	for i, part := range msg {
		if len(part) == 0 && len(msg) == i+2 {
			return msg[:i], msg[i+1], nil
		} else if len(part) == 0 {
			return nil, nil, errors.New("Invalid front msg")
		}
	}
	return nil, nil, errors.New("Invalid front msg")
}

func (self *Balancer) loadBalance() (*Worker, error) {
	if len(self.workers) == 0 {
		return nil, errors.New("No workers")
	} else {
		keys := make([]string, 0, len(self.workers))
		for k, w := range self.workers {
			if w.state == ONLINE {
				keys = append(keys, k)
			}
		}
		random_key := keys[rand.Intn(len(keys))]
		return self.workers[random_key], nil
	}
}

func (self *Balancer) handleFront(msg [][]byte) error {
	route, body, err := frontRequest(msg)
	if err != nil {
		return err
	}
	worker, err := self.loadBalance()
	if err != nil {
		self.front.SendMessage(route, "", "")
		return err
	}
	err = worker.SendRequest(self.front, route, body)
	if err != nil {
		self.front.SendMessage(route, "", "")
		return err
	}
	return nil
}

func (self *Balancer) handleBack(msg [][]byte) error {
	workerId := string(msg[0])
	command := string(msg[1])
	switch command {
	case "READY":
		self.workers[workerId] = NewWorker(workerId, self)
	case "HEARTBEAT":
		worker, pst := self.workers[workerId]
		if pst == false {
			return errors.New("Can't find worker")
		}
		worker.HeartbeatReceived()
	case "REPLY":
		worker, pst := self.workers[workerId]
		if pst == false {
			return errors.New("Can't find worker")
		}
		return worker.Reply(string(msg[2]), msg[3])
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

func (self *Balancer) startHeartbeater() {
	for {
		to_disconnect := make([]*Worker, 0, len(self.workers))
		for _, w := range self.workers {
			if time.Since(w.heartbeatSent).Seconds() > 3 {
				w.SendHeartbeat()
			}

			if time.Since(w.heartbeatReceived).Seconds() > 10 {
				to_disconnect = append(to_disconnect, w)
			}
		}

		for _, w := range to_disconnect {
			w.Disconnect()
			delete(self.workers, w.id)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (self *Balancer) Run() {
	go self.startHeartbeater()
	poller := zmq4.NewPoller()
	poller.Add(self.front, zmq4.POLLIN)
	poller.Add(self.back, zmq4.POLLIN)
	for {
		if !self.system.Running.Load().(bool) {
			return
		}
		sockets, _ := poller.Poll(100 * time.Millisecond)
		for _, socket := range sockets {

			var err error
			switch socket.Socket {
			case self.front:
				err = self.handleMessages(self.front, self.handleFront)
			case self.back:
				err = self.handleMessages(self.back, self.handleBack)
			}
			if err != nil {
				log.WithFields(log.Fields{
					"error":    err,
					"balancer": self.name,
				}).Error("Message handling error")

			}
		}
	}
}
