package ack

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrMsgRecordFailed = errors.New("the buffer is full, asynchronously record msg failed")
	ErrMsgAckFailed    = errors.New("the buffer is full, asynchronously ack msg failed")
)

type AckManager struct {
	capacity int
	records  []*recorder
	canAck   CanAck

	async  bool
	setCh  chan *msg
	ackCh  chan *msg
	stopCh chan struct{}
	status int32
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
			a.set(m.ID, m.Flag, m.Value)
		case m := <-a.ackCh:
			a.ack(m.ID, m.Flag)
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

func (a *AckManager) Set(id int64, flag, val interface{}) error {
	if a.async {
		m := &msg{
			ID:    id,
			Time:  time.Now().UnixNano(),
			Flag:  flag,
			Value: val,
		}
		select {
		case a.setCh <- m:
			return nil
		default:
			return ErrMsgRecordFailed
		}
	}

	a.set(id, flag, val)
	return nil
}

func (a *AckManager) set(id int64, flag, val interface{}) {
	index := id % int64(a.capacity)
	a.records[index].Set(id, flag, val)
}

func (a *AckManager) Ack(id int64, flag interface{}) error {
	if a.async {
		m := &msg{
			ID:   id,
			Flag: flag,
		}
		select {
		case a.ackCh <- m:
			return nil
		default:
			return ErrMsgAckFailed
		}
	}

	a.ack(id, flag)
	return nil
}

func (a *AckManager) ack(id int64, flag interface{}) {
	index := id % int64(a.capacity)
	a.records[index].Remove(id, flag)
}

func (a *AckManager) Get(duration int64) []*msg {
	var res []*msg
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
