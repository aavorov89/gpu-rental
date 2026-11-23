// Package payoutstest provides an in-memory payment provider for tests.
package payoutstest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gpu-rental/internal/payouts"
)

// Outcome is what the provider does with one CreateTransfer call.
type Outcome int

const (
	// Success moves the money and reports success.
	Success Outcome = iota
	// HardFailure definitively rejects the transfer. No money moves.
	HardFailure
	// TimeoutNotApplied loses the request. No money moves, but the caller
	// only sees an ambiguous error.
	TimeoutNotApplied
	// TimeoutApplied moves the money and loses the response, so the caller
	// sees the same ambiguous error.
	TimeoutApplied
)

var (
	// ErrAmbiguous says nothing about whether the transfer happened.
	ErrAmbiguous = errors.New("ambiguous provider response")
	// ErrKeyReused means one idempotency key was sent with different parameters.
	ErrKeyReused = errors.New("idempotency key reused with different parameters")
)

// CreateCall records the arguments of one CreateTransfer call.
type CreateCall struct {
	Key    string
	HostID int64
	Amount int64
}

type transfer struct {
	hostID int64
	amount int64
	status payouts.Status
}

type gate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) open() {
	g.once.Do(func() { close(g.release) })
}

func (g *gate) wait(ctx context.Context) error {
	close(g.entered)

	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrAmbiguous, ctx.Err())
	}
}

// Provider implements payouts.Provider and deduplicates transfers by key,
// as a real idempotent payment API would.
type Provider struct {
	mu             sync.Mutex
	createOutcomes []Outcome
	lookupErrors   []error
	transfers      map[string]*transfer
	applied        map[string]int
	createCalls    []CreateCall
	lookupCalls    []string
	activeLookups  map[string]int
	lookupOverlap  bool
	lookupDelay    time.Duration
	createGate     *gate
	lookupGate     *gate
}

// New returns a provider whose calls succeed until told otherwise.
func New() *Provider {
	return &Provider{
		transfers:     make(map[string]*transfer),
		applied:       make(map[string]int),
		activeLookups: make(map[string]int),
	}
}

// QueueCreate sets the outcomes of the next CreateTransfer calls.
// Once the queue is empty, calls succeed.
func (p *Provider) QueueCreate(outcomes ...Outcome) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.createOutcomes = append(p.createOutcomes, outcomes...)
}

// QueueLookupError makes the next LookupTransfer calls return these errors.
func (p *Provider) QueueLookupError(errs ...error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupErrors = append(p.lookupErrors, errs...)
}

// SetLookupDelay makes every lookup take at least d.
func (p *Provider) SetLookupDelay(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupDelay = d
}

// BlockNextCreate pauses the next CreateTransfer. The channel closes when the
// call arrives, and the function lets it continue.
func (p *Provider) BlockNextCreate() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.createGate = newGate()

	return p.createGate.entered, p.createGate.open
}

// BlockNextLookup pauses the next LookupTransfer in the same way.
func (p *Provider) BlockNextLookup() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupGate = newGate()

	return p.lookupGate.entered, p.lookupGate.open
}

// CreateTransfer implements payouts.Provider.
func (p *Provider) CreateTransfer(ctx context.Context, key string, hostID int64, amount int64) error {
	p.mu.Lock()
	p.createCalls = append(p.createCalls, CreateCall{Key: key, HostID: hostID, Amount: amount})
	g := p.createGate
	p.createGate = nil
	p.mu.Unlock()

	if g != nil {
		if err := g.wait(ctx); err != nil {
			return err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	outcome := Success
	if len(p.createOutcomes) > 0 {
		outcome = p.createOutcomes[0]
		p.createOutcomes = p.createOutcomes[1:]
	}

	if outcome == TimeoutNotApplied {
		return ErrAmbiguous
	}

	t, exists := p.transfers[key]
	if exists && (t.hostID != hostID || t.amount != amount) {
		return ErrKeyReused
	}

	if !exists {
		t = &transfer{hostID: hostID, amount: amount, status: payouts.StatusPaid}
		if outcome == HardFailure {
			t.status = payouts.StatusFailed
		} else {
			p.applied[key]++
		}
		p.transfers[key] = t
	}

	if t.status == payouts.StatusFailed {
		return payouts.ErrTransferRejected
	}

	if outcome == TimeoutApplied {
		return ErrAmbiguous
	}

	return nil
}

// LookupTransfer implements payouts.Provider.
func (p *Provider) LookupTransfer(ctx context.Context, key string) (payouts.Status, error) {
	p.mu.Lock()
	p.lookupCalls = append(p.lookupCalls, key)
	p.activeLookups[key]++
	if p.activeLookups[key] > 1 {
		p.lookupOverlap = true
	}
	delay := p.lookupDelay
	g := p.lookupGate
	p.lookupGate = nil

	var lookupErr error
	if len(p.lookupErrors) > 0 {
		lookupErr = p.lookupErrors[0]
		p.lookupErrors = p.lookupErrors[1:]
	}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.activeLookups[key]--
		p.mu.Unlock()
	}()

	if g != nil {
		if err := g.wait(ctx); err != nil {
			return "", err
		}
	}

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return "", fmt.Errorf("%w: %v", ErrAmbiguous, ctx.Err())
		}
	}

	if lookupErr != nil {
		return "", lookupErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	t, ok := p.transfers[key]
	if !ok {
		return "", payouts.ErrTransferNotFound
	}

	return t.status, nil
}

// CreateCalls returns every CreateTransfer call in order.
func (p *Provider) CreateCalls() []CreateCall {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]CreateCall(nil), p.createCalls...)
}

// LookupCalls returns the key of every LookupTransfer call in order.
func (p *Provider) LookupCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.lookupCalls...)
}

// Applied returns how many times money actually moved for key.
func (p *Provider) Applied(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.applied[key]
}

// LookupOverlapped reports whether two lookups for one key ever ran at once.
func (p *Provider) LookupOverlapped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lookupOverlap
}
