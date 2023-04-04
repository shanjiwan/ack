package ack

import (
	"sync"
	"time"
)

// recorder records messages
type recorder struct {
	sync.RWMutex
	msgs map[int64]int64 // uid => timestamp
}

func newRecorder() *recorder {
	return &recorder{
		msgs: map[int64]int64{},
	}
}

// Set uid and timestamp
func (r *recorder) Set(uid, time int64) {
	r.Lock()
	r.msgs[uid] = time
	r.Unlock()
}

// Remove uid if timestamp older than the record one
func (r *recorder) Remove(uid, time int64) {
	r.Lock()
	t, ok := r.msgs[uid]
	if ok && t <= time {
		delete(r.msgs, uid)
	}
	r.Unlock()
}

// Get uid list have not removed after duration
func (r *recorder) Get(duration int64) []int64 {
	if duration <= 0 {
		return []int64{}
	}

	res := make([]int64, 0)
	now := time.Now().UnixNano()
	r.RLock()
	for u, t := range r.msgs {
		if now-t >= duration {
			res = append(res, u)
		}
	}
	r.RUnlock()
	return res
}

// ReAllocate to release the msgs memory
func (r *recorder) ReAllocate() {
	newMsgs := make(map[int64]int64, len(r.msgs))
	r.Lock()
	for k, v := range r.msgs {
		newMsgs[k] = v
	}
	r.msgs = newMsgs
	r.RUnlock()
}
