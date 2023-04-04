package ack

import "errors"

type AckManager struct {
	capacity int
	records  []*recorder
}

func NewAckManager(capacity int) (*AckManager, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity should be more than 0")
	}
	am := &AckManager{
		capacity: capacity,
		records:  make([]*recorder, 0, capacity),
	}

	for i := 0; i < capacity; i++ {
		am.records = append(am.records, newRecorder())
	}
	return am, nil
}

func (a *AckManager) Set(uid, time int64) {
	index := uid % int64(a.capacity)
	a.records[index].Set(uid, time)
}

func (a *AckManager) Remove(uid, time int64) {
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
