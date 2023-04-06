package ack

import (
	"sync"
	"time"
)

// recorder records messages
type recorder struct {
	sync.RWMutex
	msgs map[int64]*msg // msgID => msg
	am   *AckManager
}

func newRecorder() *recorder {
	return &recorder{
		msgs: map[int64]*msg{},
	}
}

// Set uid and timestamp
func (r *recorder) Set(id int64, flag, val interface{}) {
	r.Lock()
	m := &msg{
		ID:    id,
		Time:  time.Now().UnixNano(),
		Flag:  flag,
		Value: val,
	}
	r.msgs[id] = m
	r.Unlock()
}

// Remove uid if timestamp older than the record one
func (r *recorder) Remove(id int64, flag interface{}) {
	r.Lock()
	m, ok := r.msgs[id]
	if ok && r.am.canAck(m.Flag, flag) {
		delete(r.msgs, id)
	}
	r.Unlock()
}

// Get uid list have not removed after duration
func (r *recorder) Get(duration int64) []*msg {
	if duration <= 0 {
		return []*msg{}
	}

	res := make([]*msg, 0)
	now := time.Now().UnixNano()
	r.RLock()
	for _, m := range r.msgs {
		if now-m.Time >= duration {
			res = append(res, m)
		}
	}
	r.RUnlock()
	return res
}

// ReAllocate to release the msgs memory
func (r *recorder) ReAllocate() {
	newMsgs := make(map[int64]*msg, len(r.msgs))
	r.Lock()
	for k, v := range r.msgs {
		newMsgs[k] = v
	}
	r.msgs = newMsgs
	r.RUnlock()
}
