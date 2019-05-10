package picker

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

const (
	Invalid = -1
)

var defaultPeer = Peer{
	Weight:      1,
	MaxFails:    1,
	FailTimeout: 10 * time.Second,
}

type CheckFunc func(i int, data interface{}) bool

type context struct {
	Tries     int
	CheckFunc CheckFunc

	tried   []bool
	picker  *Picker
	current *Peer
}

type Picker struct {
	sync.RWMutex

	CheckFunc CheckFunc

	peers []*Peer

	inited bool
}

// New 创建一个新的 Picker
func New(n int) *Picker {
	p := &Picker{}
	if n > 0 {
		p.peers = make([]*Peer, n)

		for i := 0; i < n; i++ {
			peer := defaultPeer

			peer.index = i
			peer.Name = fmt.Sprintf("peer_%v", i)

			p.peers[i] = &peer
		}
	}

	return p
}

func (p *Picker) Get(i int) *Peer {
	return p.peers[i]
}

func (p *Picker) Add() *Peer {
	peer := defaultPeer

	peer.index = len(p.peers)
	peer.Name = fmt.Sprintf("peer_%v", peer.index)

	p.peers = append(p.peers, &peer)

	return &peer
}

func (p *Picker) Context() *context {
	return &context{
		picker: p,
		Tries:  len(p.peers),
	}
}

type Peer struct {
	sync.Mutex

	Name string

	Weight         int64
	MaxFails       uint64
	FailTimeout    time.Duration
	MaxConnections uint64
	Data           interface{}

	index int

	currentWeight   int64
	effectiveWeight int64

	connections uint64

	fails    uint64
	accessed time.Time
	checked  time.Time
}

func (p *Picker) init() {
	for _, peer := range p.peers {
		peer.effectiveWeight = peer.Weight
	}

	p.inited = true
}

func (p *Picker) pick(ctx *context) *Peer {
	var (
		best  *Peer
		index int
		total int64
	)

	if ctx.tried == nil {
		ctx.tried = make([]bool, len(p.peers))
	}

	for i, peer := range p.peers {
		if ctx.tried[i] {
			continue
		}

		if ctx.CheckFunc != nil && !ctx.CheckFunc(i, peer.Data) {
			continue
		}

		if p.CheckFunc != nil && !p.CheckFunc(i, peer.Data) {
			continue
		}

		if peer.MaxConnections > 0 && peer.connections >= peer.MaxConnections {
			continue
		}

		if peer.MaxFails > 0 && peer.fails >= peer.MaxFails {
			if time.Since(peer.checked) <= peer.FailTimeout {
				continue
			}

			glog.Infof("try failed peer %v", peer.Name)
		}

		peer.currentWeight += peer.effectiveWeight
		total += peer.effectiveWeight

		if peer.effectiveWeight < peer.Weight {
			peer.effectiveWeight++
		}

		if best == nil || peer.currentWeight > best.currentWeight {
			best = peer
			index = i
		}
	}

	if best == nil {
		return nil
	}

	ctx.tried[index] = true

	best.currentWeight -= total

	if time.Since(best.checked) > best.FailTimeout {
		best.checked = time.Now()
	}

	return best
}

func (p *Picker) Pick(ctx *context) *Peer {
	if ctx.current != nil {
		glog.Fatalf("previous peer not released: %v", ctx.current)
	}

	if ctx.Tries == 0 {
		return nil
	}

	p.Lock()
	defer p.Unlock()

	if len(p.peers) == 0 {
		return nil
	}

	if !p.inited {
		p.init()
	}

	var peer *Peer

	if len(p.peers) == 1 {
		peer = p.peers[0]

		if peer.MaxConnections > 0 && peer.connections >= peer.MaxConnections {
			goto failed
		}

		if ctx.CheckFunc != nil && !ctx.CheckFunc(0, peer.Data) {
			goto failed
		}
	} else {
		peer = p.pick(ctx)

		if peer == nil {
			goto failed
		}

		glog.Infof("get rr peer, current: %v %v", peer.Name, peer.currentWeight)
	}

	peer.connections++

	ctx.current = peer

	return peer

failed:

	return nil
}

func (p *Picker) release(ctx *context, ok bool) {
	peer := ctx.current

	if peer == nil {
		glog.Fatal("peer release with no current")
		return
	}

	p.RLock()
	peer.Lock()

	if len(p.peers) == 1 {
		peer.connections--

		peer.Unlock()
		p.RUnlock()

		ctx.Tries = 0
		ctx.current = nil

		return
	}

	if !ok {
		now := time.Now()

		peer.fails++
		peer.accessed = now
		peer.checked = now

		if peer.MaxFails > 0 {
			peer.effectiveWeight -= peer.Weight / int64(peer.MaxFails)

			if peer.fails >= peer.MaxFails {
				glog.Errorf("peer %v temporarily disabled", peer.Name)
			}
		}

		glog.Infof("free failed peer: %v, %v", peer.Name, peer.effectiveWeight)

		if peer.effectiveWeight < 0 {
			peer.effectiveWeight = 0
		}
	} else {
		if peer.accessed.Before(peer.checked) {
			peer.fails = 0

			glog.Infof("peer %v is up", peer.Name)
		}
	}

	peer.connections--

	peer.Unlock()
	p.RUnlock()

	if ctx.Tries > 0 {
		ctx.Tries--
	}
	ctx.current = nil
}

func (c *context) Pick() (int, interface{}) {
	peer := c.picker.Pick(c)
	if peer == nil {
		return Invalid, nil
	}

	return peer.index, peer.Data
}

func (c *context) Release(ok bool) {
	c.picker.release(c, ok)
}

func (c *context) Close() {
	if c.current != nil {
		glog.Fatalf("last peer not released in picker. %#v", c.current)
	}
}
