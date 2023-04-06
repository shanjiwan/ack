package ack

type msg struct {
	ID    int64
	Time  int64
	Flag  interface{}
	Value interface{}
}
