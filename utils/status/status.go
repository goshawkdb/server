package status

import (
	"goshawkdb.io/common"
	"strings"
	"sync"
)

type StatusEmitter interface {
	Status(*StatusConsumer)
}

type StatusConsumer struct {
	wg    *common.ChannelWaitGroup
	sep   string
	lock  sync.Mutex
	slots [][]string
}

func NewStatusConsumer() *StatusConsumer {
	wg := common.NewChannelWaitGroup()
	wg.Add(1)
	return &StatusConsumer{
		wg:    wg,
		sep:   "\n ",
		slots: make([][]string, 0, 8),
	}
}

func (s *StatusConsumer) Fork() *StatusConsumer {
	s.wg.Add(1)
	sc := NewStatusConsumer()
	sc.sep = s.sep + " "
	s.lock.Lock()
	slotIdx := len(s.slots)
	s.slots = append(s.slots, nil)
	s.lock.Unlock()
	go func() {
		str := sc.Wait()
		s.lock.Lock()
		s.slots[slotIdx] = []string{str}
		s.lock.Unlock()
		s.wg.Done()
	}()
	return sc
}

func (s *StatusConsumer) Join() {
	s.wg.Done()
}

func (s *StatusConsumer) Emit(status ...string) {
	s.lock.Lock()
	s.slots = append(s.slots, status)
	s.lock.Unlock()
}

func (s *StatusConsumer) Wait() string {
	s.wg.Wait()
	buf := " "
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
