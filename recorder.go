package ack

import (
	"sync"
	"time"
)

// msg is internal encapsulation of the sending message.
type msg[flag, val any] struct {
	// message ID
	ID int64
	// Timestamp is the time when message is sent.
	Timestamp int64
	// Flag is used in some situation. see comment in Config field CanAck.
	Flag flag
	// Value is the actual sent message.
	Value val
}

// recorder records messages.
type recorder[flag, val any] struct {
	sync.RWMutex
	msgs map[int64]*msg[flag, val] // msgID => msg
	am   *AckManager[flag, val]
}

func newRecorder[flag, val any](am *AckManager[flag, val]) *recorder[flag, val] {
	return &recorder[flag, val]{
		msgs: map[int64]*msg[flag, val]{},
		am:   am,
	}
}

// Set messages.
func (r *recorder[flag, val]) Set(id int64, f flag, v val) {
	r.Lock()
	m := &msg[flag, val]{
		ID:        id,
		Timestamp: time.Now().UnixNano(),
		Flag:      f,
		Value:     v,
	}
	r.msgs[id] = m
	r.Unlock()
}

// Remove messages if canAck is true.
func (r *recorder[flag, val]) Remove(id int64, f flag) {
	r.Lock()
	m, ok := r.msgs[id]
	canAck := true
	if r.am.canAck != nil {
		canAck = r.am.canAck(m.Flag, f)
	}
	if ok && canAck {
		delete(r.msgs, id)
	}
	r.Unlock()
}

// Get messages list have not acked after duration.
func (r *recorder[flag, val]) Get(duration int64) []*msg[flag, val] {
	if duration <= 0 {
		return []*msg[flag, val]{}
	}

	res := make([]*msg[flag, val], 0)
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
func (r *recorder[flag, val]) ReAllocate() {
	newMsgs := make(map[int64]*msg[flag, val], len(r.msgs))
	r.Lock()
	for k, v := range r.msgs {
		newMsgs[k] = v
	}
	r.msgs = newMsgs
	r.RUnlock()
}
