package status

import (
	"strings"
	"sync"
	"sync/atomic"
)

type StatusConsumer struct {
	sync.Mutex
	forkCount int32
	sep       string
	slots     [][]string
	joined    chan struct{}
}

func NewStatusConsumer() *StatusConsumer {
	return &StatusConsumer{
		forkCount: 1,
		sep:       "\n ",
		slots:     make([][]string, 0, 8),
		joined:    make(chan struct{}),
	}
}

func (s *StatusConsumer) Fork() *StatusConsumer {
	atomic.AddInt32(&s.forkCount, 1)
	sc := NewStatusConsumer()
	sc.sep = s.sep + " "
	s.Lock()
	slotIdx := len(s.slots)
	s.slots = append(s.slots, nil)
	s.Unlock()
	go func() {
		str := s.Wait()
		s.Lock()
		s.slots[slotIdx] = []string{str}
		s.Unlock()
		s.Join()
	}()
	return sc
}

func (s *StatusConsumer) Join() {
	if atomic.AddInt32(&s.forkCount, -1) == 0 {
		close(s.joined)
	}
}

func (s *StatusConsumer) Emit(status ...string) {
	s.Lock()
	s.slots = append(s.slots, status)
	s.Unlock()
}

func (s *StatusConsumer) Wait() string {
	buf := " "
	<-s.joined
	for _, strs := range s.slots {
		buf += strings.Join(strs, s.sep) + s.sep
	}
	if len(buf) == 1 {
		return buf
	} else {
		end := len(buf) - len(s.sep)
		return buf[:end]
	}
}
