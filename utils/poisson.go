package utils

import (
	"goshawkdb.io/server"
	"math"
	"time"
)

type Poisson struct {
	events []time.Time
	front  int
	back   int
	length int
}

func NewPoisson() *Poisson {
	return &Poisson{
		events: make([]time.Time, server.PoissonSamples),
	}
}

func (p *Poisson) AddNow() {
	p.AddThen(time.Now())
}

func (p *Poisson) AddThen(now time.Time) {
	p.events[p.front] = now
	p.front++
	if p.front == server.PoissonSamples {
		p.front = 0
	}
	if p.front == p.back {
		p.back++
		if p.back == server.PoissonSamples {
			p.back = 0
		}
	} else {
		p.length++
	}
}

func (p *Poisson) Cull(limit time.Time) {
	for p.back != p.front {
		if p.events[p.back].Before(limit) {
			p.length--
			p.back++
			if p.back == server.PoissonSamples {
				p.back = 0
			}
		} else {
			break
		}
	}
}

func (p *Poisson) interval(now time.Time) time.Duration {
	if p.length == 0 {
		return 0
	} else {
		return now.Sub(p.events[p.back])
	}
}

// Can return NaN if length is 0
// Can return +Inf if length > 0 and interval gives 0
// Can return < 0 if length > 0 and interval gives < 0 (think: leap seconds)
func (p *Poisson) λ(now time.Time) float64 {
	return float64(p.length) / float64(p.interval(now))
}

func (p *Poisson) P(t time.Duration, k int64, now time.Time) float64 {
	//p.Cull(now.Add(-1 * time.Second))
	λt := p.λ(now) * float64(t)
	if math.IsNaN(λt) {
		return 1
	} else if math.IsInf(λt, 0) || λt < 0 {
		return 0
	} else {
		return (math.Pow(λt, float64(k)) * math.Exp(-λt)) / float64(fac(k))
	}
}

func fac(n int64) int64 {
	acc := int64(1)
	for ; n > 1; n-- {
		acc *= n
	}
	return acc
}
