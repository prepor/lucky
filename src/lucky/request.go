package lucky

import (
	"fmt"
	"time"

	"github.com/satori/go.uuid"
)

type Request struct {
	Id        string
	StartTime time.Time
	Route     [][]byte
	Payload   [][]byte
	Reply     [][]byte
	answers   chan *Request
}

func NewRequest(route [][]byte, payload [][]byte, answers chan *Request) *Request {
	return &Request{
		Id:        uuid.NewV4().String(),
		StartTime: time.Now(),
		Route:     route,
		Payload:   payload,
		answers:   answers,
	}
}

func (self *Request) Answer(reply [][]byte) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("%v", v)
			return
		}
	}()
	self.Reply = reply
	self.answers <- self
	return nil
}

func (self *Request) Error(e string) error {
	return self.Answer([][]byte{[]byte(e)})
}
