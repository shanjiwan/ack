package ack

import (
	"sync"
	"time"
)

// msg is internal encapsulation of the sending message.
type msg struct {
	// message ID
	ID int64
	// Timestamp is the time when message is sent.
	Timestamp int64
	// Flag is used in some situation. see comment in Config field CanAck.
	Flag interface{}
	// Value is the actual sent message.
	Value interface{}
}

// recorder records messages.
type recorder struct {
	sync.RWMutex
	msgs map[int64]*msg // msgID => msg
	am   *AckManager
}

func newRecorder(am *AckManager) *recorder {
	return &recorder{
		msgs: map[int64]*msg{},
		am:   am,
	}
}

// Set messages.
func (r *recorder) Set(id int64, flag, val interface{}) {
	r.Lock()
	m := &msg{
		ID:        id,
		Timestamp: time.Now().UnixNano(),
		Flag:      flag,
		Value:     val,
	}
	r.msgs[id] = m
	r.Unlock()
}

// Remove messages if canAck is true.
func (r *recorder) Remove(id int64, flag interface{}) {
	r.Lock()
	m, ok := r.msgs[id]
	canAck := true
	if r.am.canAck != nil {
		canAck = r.am.canAck(m.Flag, flag)
	}
	if ok && canAck {
		delete(r.msgs, id)
	}
	r.Unlock()
}

// Get messages list have not acked after duration.
func (r *recorder) Get(duration int64) []*msg {
	if duration <= 0 {
		return []*msg{}
	}

	res := make([]*msg, 0)
	now := time.Now().UnixNano()
	r.RLock()
	for _, m := range r.msgs {
		if now-m.Timestamp >= duration {
			res = append(res, m)
		}
	}
	r.RUnlock()
	return res
}

// ReAllocate to release the map memory.
func (r *recorder) ReAllocate() {
	newMsgs := make(map[int64]*msg, len(r.msgs))
	r.Lock()
	for k, v := range r.msgs {
		newMsgs[k] = v
	}
	r.msgs = newMsgs
	r.RUnlock()
}
