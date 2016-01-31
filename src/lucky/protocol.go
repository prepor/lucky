package lucky

import "errors"

func MsgWihDelim(msg [][]byte) (route [][]byte, body [][]byte, err error) {
	for i, part := range msg {
		if len(part) == 0 && len(msg) > i {
			return msg[:i], msg[i+1:], nil
		} else if len(part) == 0 {
			return nil, nil, errors.New("Invalid msg")
		}
	}
	return nil, nil, errors.New("Invalid msg")
}
