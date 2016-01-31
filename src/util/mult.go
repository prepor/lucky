package util

import "errors"

type MultCommandTap struct {
	ch      chan interface{}
	isClose bool
}

type MultCommandUntap struct {
	ch chan interface{}
}

type Mult struct {
	source    chan interface{}
	listeners map[chan interface{}]bool
	commands  chan interface{}
}

func NewMult(source chan interface{}) *Mult {
	self := &Mult{source: source,
		listeners: make(map[chan interface{}]bool, 10),
		commands:  make(chan interface{}),
	}
	go self.loop()
	return self
}

func (self *Mult) Tap(ch chan interface{}, isClose bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("mut closed")
			return
		}
	}()
	self.commands <- &MultCommandTap{ch: ch, isClose: isClose}
	return nil
}

func (self *Mult) Untap(ch chan interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("mut closed")
			return
		}
	}()
	self.commands <- &MultCommandUntap{ch: ch}
	return nil
}

func (self *Mult) loop() {
	for {
		select {
		case v := <-self.commands:
			switch t := v.(type) {
			case *MultCommandTap:
				self.listeners[t.ch] = t.isClose
			case *MultCommandUntap:
				delete(self.listeners, t.ch)
			}
		case v, ok := <-self.source:
			if !ok {
				for k, isClose := range self.listeners {
					if isClose {
						close(k)
					}

					delete(self.listeners, k)
				}
				close(self.commands)
				return
			}
			for k, _ := range self.listeners {
				k <- v
			}
		}

	}
}
