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

type Config struct {
	// segment lock is used to increase concurrency. Record messages are hashed to different
	// segments by message id. Capacity is the number of segments ack manager used. It must
	// be bigger than 0.
	Capacity int
	// Ack manager provide two working modes: sync mode\async mode. Sync mode is the default one.
	// When working in async mode, messages set or ack are sent to a buffer and asynchronously
	// processed. Messages set or ack will be aborted and return error when the buffer is full.
	Async bool
	// SetBufferSize and AckBufferSize is the number of messages can be buffered. It only works in
	// Async mode.
	SetBufferSize int64
	AckBufferSize int64
	// CanAck is an optional config cooperating with flag arg of Set() and Ack(). It is used in
	// some special situations.
	// For example, we need to send user state to another progress and user state only have one field
	// wallet balance. Uid is chosen as message id. Two messages was sent. First is {id:123, balance:100.00},
	// second is {id:123, balance:120.00}.We only concern the second message because if the second messages
	// is arrived, the user state is synced even if the first is lost.
	// In this situation, we can only record and ack the newest message. So we can use timestamp as flag to
	// identify if messages are newest. CanAck is like below:
	// func canAck(setFlag, ackFlag interface{}) bool {
	//		setT := setFlag.(int64)
	//      ackT := ackFlag.(int64)
	//      return setT <= ackT
	// }
	// When response of first msg arrived, it won't be acked since it not the newest.
	CanAck CanAck
}

type CanAck func(setFlag, ackFlag interface{}) bool

type AckManager struct {
	capacity int
	records  []*recorder
	canAck   CanAck

	// used for async mode
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
		am.records = append(am.records, newRecorder(am))
	}

	if cfg.Async {
		am.async = true
		am.setCh = make(chan *msg, cfg.SetBufferSize)
		am.ackCh = make(chan *msg, cfg.AckBufferSize)
	}
	return am, nil
}

// Start starts daemon goroutine in async mode.
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

// Stop stops daemon goroutine in async mode.
func (a *AckManager) Stop() {
	if !a.async || !atomic.CompareAndSwapInt32(&a.status, 1, 0) {
		return
	}
	close(a.stopCh)
}

func (a *AckManager) Set(id int64, flag, val interface{}) error {
	if a.async {
		m := &msg{
			ID:        id,
			Timestamp: time.Now().UnixNano(),
			Flag:      flag,
			Value:     val,
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
