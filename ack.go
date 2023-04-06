package ack

import (
	"errors"
	"sync/atomic"
)

var (
	ErrMsgRecordFailed = errors.New("the buffer is full, asynchronously record msg failed")
	ErrMsgAckFailed    = errors.New("the buffer is full, asynchronously ack msg failed")
)

type AckManager struct {
	capacity int
	records  []*recorder

	async  bool
	setCh  chan *msg
	ackCh  chan *msg
	stopCh chan struct{}
	status int32
}

type msg struct {
	Uid  int64
	Time int64
}

func NewAckManager(cfg *Config) (*AckManager, error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("capacity should be more than 0")
	}
	am := &AckManager{
		capacity: cfg.Capacity,
		records:  make([]*recorder, 0, cfg.Capacity),
	}
	for i := 0; i < cfg.Capacity; i++ {
		am.records = append(am.records, newRecorder())
	}

	if cfg.Async {
		am.async = true
		am.setCh = make(chan *msg, 1024)
		am.ackCh = make(chan *msg, 1024)
	}
	return am, nil
}

func (a *AckManager) Start() {
	if !a.async || !atomic.CompareAndSwapInt32(&a.status, 0, 1) {
		return
	}

	a.stopCh = make(chan struct{})
	go func() {
		select {
		case m := <-a.setCh:
			a.set(m.Uid, m.Time)
		case m := <-a.ackCh:
			a.ack(m.Uid, m.Time)
		case <-a.stopCh:
			return
		}
	}()
}

func (a *AckManager) Stop() {
	if !a.async || !atomic.CompareAndSwapInt32(&a.status, 1, 0) {
		return
	}
	close(a.stopCh)
}

func (a *AckManager) Set(uid, time int64) error {
	if a.async {
		m := &msg{
			Uid:  uid,
			Time: time,
		}
		select {
		case a.setCh <- m:
			return nil
		default:
			return ErrMsgRecordFailed
		}
	}

	a.set(uid, time)
	return nil
}

func (a *AckManager) set(uid, time int64) {
	index := uid % int64(a.capacity)
	a.records[index].Set(uid, time)
}

func (a *AckManager) Ack(uid, time int64) error {
	if a.async {
		m := &msg{
			Uid:  uid,
			Time: time,
		}
		select {
		case a.ackCh <- m:
			return nil
		default:
			return ErrMsgAckFailed
		}
	}

	a.ack(uid, time)
	return nil
}

func (a *AckManager) ack(uid, time int64) {
	index := uid % int64(a.capacity)
	a.records[index].Remove(uid, time)
}

func (a *AckManager) Get(duration int64) []int64 {
	var res []int64
	for _, r := range a.records {
		res = append(res, r.Get(duration)...)
	}
	return res
}

func (a *AckManager) ReAllocate() {
	for _, v := range a.records {
		v.ReAllocate()
	}
}
