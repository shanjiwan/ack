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

type Config[flag any] struct {
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
	CanAck CanAck[flag]
}

type CanAck[flag any] func(setFlag, ackFlag flag) bool

type AckManager[flag, val any] struct {
	capacity int
	records  []*recorder[flag, val]
	canAck   CanAck[flag]

	// used for async mode
	async  bool
	setCh  chan *msg[flag, val]
	ackCh  chan *msg[flag, val]
	stopCh chan struct{}
	status int32
}

func NewAckManager[flag, val any](cfg *Config[flag]) (*AckManager[flag, val], error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("capacity should be more than 0")
	}
	am := &AckManager[flag, val]{
		capacity: cfg.Capacity,
		records:  make([]*recorder[flag, val], 0, cfg.Capacity),
		canAck:   cfg.CanAck,
	}
	for i := 0; i < cfg.Capacity; i++ {
		am.records = append(am.records, newRecorder[flag, val](am))
	}

	if cfg.Async {
		am.async = true
		am.setCh = make(chan *msg[flag, val], cfg.SetBufferSize)
		am.ackCh = make(chan *msg[flag, val], cfg.AckBufferSize)
	}
	return am, nil
}

// Start starts daemon goroutine in async mode.
func (a *AckManager[flag, val]) Start() {
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
func (a *AckManager[flag, val]) Stop() {
	if !a.async || !atomic.CompareAndSwapInt32(&a.status, 1, 0) {
		return
	}
	close(a.stopCh)
}

func (a *AckManager[flag, val]) Set(id int64, f flag, v val) error {
	if a.async {
		m := &msg[flag, val]{
			ID:        id,
			Timestamp: time.Now().UnixNano(),
			Flag:      f,
			Value:     v,
		}
		select {
		case a.setCh <- m:
			return nil
		default:
			return ErrMsgRecordFailed
		}
	}

	a.set(id, f, v)
	return nil
}

func (a *AckManager[flag, val]) set(id int64, f flag, v val) {
	index := id % int64(a.capacity)
	a.records[index].Set(id, f, v)
}

func (a *AckManager[flag, val]) Ack(id int64, f flag) error {
	if a.async {
		m := &msg[flag, val]{
			ID:   id,
			Flag: f,
		}
		select {
		case a.ackCh <- m:
			return nil
		default:
			return ErrMsgAckFailed
		}
	}

	a.ack(id, f)
	return nil
}

func (a *AckManager[flag, val]) ack(id int64, f flag) {
	index := id % int64(a.capacity)
	a.records[index].Remove(id, f)
}

func (a *AckManager[flag, val]) Get(duration int64) []*msg[flag, val] {
	var res []*msg[flag, val]
	for _, r := range a.records {
		res = append(res, r.Get(duration)...)
	}
	return res
}

func (a *AckManager[flag, val]) ReAllocate() {
	for _, v := range a.records {
		v.ReAllocate()
	}
}
