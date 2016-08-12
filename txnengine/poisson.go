package txnengine

import (
	"goshawkdb.io/server"
	"math"
	"time"
)

type Poisson struct {
	events []time.Time
	ptr    int
}

func NewPoisson() *Poisson {
	return &Poisson{
		events: make([]time.Time, 0, server.PoissonSamples),
		ptr:    0,
	}
}

func (p *Poisson) AddNow() {
	p.AddThen(time.Now())
}

func (p *Poisson) AddThen(now time.Time) {
	l, c := len(p.events), cap(p.events)
	switch {
	case l < c:
		p.events = append(p.events, now)
	default:
		p.events[p.ptr] = now
		p.ptr++
		if p.ptr == l {
			p.ptr = 0
		}
	}
}

func (p *Poisson) interval(now time.Time) time.Duration {
	l, c := len(p.events), cap(p.events)
	switch {
	case l == 0:
		return 0
	case l < c:
		return now.Sub(p.events[0])
	default:
		oldest := p.ptr + 1
		if oldest == l {
			oldest = 0
		}
		return now.Sub(p.events[oldest])
	}
}

func (p *Poisson) length() float64 {
	if l, c := len(p.events), cap(p.events); l < c {
		return float64(l)
	} else {
		return float64(c)
	}
}

func (p *Poisson) λ(now time.Time) float64 {
	return p.length() / float64(p.interval(now))
}

func (p *Poisson) P(t time.Duration, k int64) float64 {
	now := time.Now()
	λt := p.λ(now) * float64(t)
	if math.IsNaN(λt) {
		return 1
	}
	return (math.Pow(λt, float64(k)) * math.Exp(-λt)) / float64(fac(k))
}

func fac(n int64) int64 {
	acc := int64(1)
	for ; n > 1; n-- {
		acc *= n
	}
	return acc
}
