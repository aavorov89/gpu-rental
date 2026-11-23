// Package payoutstest has a mock provider that keeps transfers in memory.
package payoutstest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gpu-rental/internal/payouts"
)

// Outcome tells the mock how to handle a transfer request.
type Outcome int

const (
	// Success pays the host and sends back a reply.
	Success Outcome = iota
	// HardFailure rejects a new transfer and saves its status as failed.
	HardFailure
	// TimeoutNotApplied drops the request before payment. The caller gets ErrAmbiguous.
	TimeoutNotApplied
	// TimeoutApplied pays the host and drops the reply. The caller gets ErrAmbiguous.
	TimeoutApplied
	// InProgress accepts a transfer that stays pending until SetStatus changes it.
	InProgress
)

var (
	// ErrAmbiguous means the caller still has to check if the money moved.
	ErrAmbiguous = errors.New("ambiguous provider response")
	// ErrKeyReused means this key already belongs to another host or amount.
	ErrKeyReused = errors.New("idempotency key reused with different parameters")
)

// CreateCall holds the key, host and amount from one request.
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

// Provider remembers each transfer by key. A retry with the same key pays at most once.
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

// New makes a mock that accepts new transfers by default.
func New() *Provider {
	return &Provider{
		transfers:     make(map[string]*transfer),
		applied:       make(map[string]int),
		activeLookups: make(map[string]int),
	}
}

// QueueCreate adds results for upcoming transfer requests, in order.
// After the last result, new transfers succeed again.
func (p *Provider) QueueCreate(outcomes ...Outcome) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.createOutcomes = append(p.createOutcomes, outcomes...)
}

// QueueLookupError adds one error per lookup. A nil entry allows a normal lookup.
func (p *Provider) QueueLookupError(errs ...error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupErrors = append(p.lookupErrors, errs...)
}

// SetLookupDelay adds a wait before the mock returns a lookup result.
func (p *Provider) SetLookupDelay(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupDelay = d
}

// BlockNextCreate pauses the next transfer request. Wait for the channel to
// close, then call the returned function when the request can continue.
func (p *Provider) BlockNextCreate() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.createGate = newGate()

	return p.createGate.entered, p.createGate.open
}

// BlockNextLookup pauses one status check. The closed channel tells the test
// the check has started. Call the returned function to let it finish.
func (p *Provider) BlockNextLookup() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupGate = newGate()

	return p.lookupGate.entered, p.lookupGate.open
}

// CreateTransfer records each request. Repeating it with the same key,
// host and amount can move the money only once.
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
		switch outcome {
		case HardFailure:
			t.status = payouts.StatusFailed
		case InProgress:
			t.status = payouts.StatusPending
		default:
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

// LookupTransfer reads the saved transfer for this key. A missing key gives ErrTransferNotFound.
func (p *Provider) LookupTransfer(ctx context.Context, key string) (payouts.Transfer, error) {
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
			return payouts.Transfer{}, err
		}
	}

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return payouts.Transfer{}, fmt.Errorf("%w: %v", ErrAmbiguous, ctx.Err())
		}
	}

	if lookupErr != nil {
		return payouts.Transfer{}, lookupErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	t, ok := p.transfers[key]
	if !ok {
		return payouts.Transfer{}, payouts.ErrTransferNotFound
	}

	return payouts.Transfer{HostID: t.hostID, Amount: t.amount, Status: t.status}, nil
}

// SetStatus changes a saved transfer, as if the provider finished it later.
func (p *Provider) SetStatus(key string, status payouts.Status) {
	p.mu.Lock()
	defer p.mu.Unlock()

	t, ok := p.transfers[key]
	if !ok {
		panic(fmt.Sprintf("payoutstest: SetStatus: no transfer for key %q", key))
	}

	if t.status == payouts.StatusPending && status == payouts.StatusPaid {
		p.applied[key]++
	}
	t.status = status
}

// CreateCalls lists the transfer requests in the order the mock received them.
func (p *Provider) CreateCalls() []CreateCall {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]CreateCall(nil), p.createCalls...)
}

// LookupCalls lists the keys in the order the mock received the status checks.
func (p *Provider) LookupCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.lookupCalls...)
}

// Applied counts how many times the mock paid the host for this key.
func (p *Provider) Applied(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.applied[key]
}

// LookupOverlapped reports if two status checks ran at the same time for the same key.
func (p *Provider) LookupOverlapped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lookupOverlap
}
