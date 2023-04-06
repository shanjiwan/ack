package ack

type Config struct {
	Capacity int
	Async    bool
	Compare  CanAck
}

type CanAck func(setFlag, ackFlag interface{}) bool
