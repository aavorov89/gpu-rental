package payoutstest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gpu-rental/internal/payouts"
)

type Outcome int

const (
	Success Outcome = iota
	HardFailure
	TimeoutNotApplied
	TimeoutApplied
	// InProgress stays pending until the test calls SetStatus.
	InProgress
)

var (
	// Both timeout cases return ErrAmbiguous. Only a lookup shows if payment happened.
	ErrAmbiguous = errors.New("ambiguous provider response")
	ErrKeyReused = errors.New("idempotency key reused with different parameters")
)

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

func New() *Provider {
	return &Provider{
		transfers:     make(map[string]*transfer),
		applied:       make(map[string]int),
		activeLookups: make(map[string]int),
	}
}

// QueueCreate sets outcomes in call order. Retries also use a queue entry.
// When the queue is empty, new transfers succeed.
func (p *Provider) QueueCreate(outcomes ...Outcome) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.createOutcomes = append(p.createOutcomes, outcomes...)
}

// Use a nil entry in QueueLookupError for a lookup that should read the saved status.
func (p *Provider) QueueLookupError(errs ...error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupErrors = append(p.lookupErrors, errs...)
}

func (p *Provider) SetLookupDelay(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupDelay = d
}

// BlockNextCreate closes the channel when the create call arrives.
// Call the returned function to release it.
func (p *Provider) BlockNextCreate() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.createGate = newGate()

	return p.createGate.entered, p.createGate.open
}

// BlockNextLookup uses the same channel and release convention as BlockNextCreate.
func (p *Provider) BlockNextLookup() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lookupGate = newGate()

	return p.lookupGate.entered, p.lookupGate.open
}

// CreateTransfer records each retry. For the same key, host and amount,
// the mock pays at most once.
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

func (p *Provider) CreateCalls() []CreateCall {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]CreateCall(nil), p.createCalls...)
}

func (p *Provider) LookupCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.lookupCalls...)
}

func (p *Provider) Applied(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.applied[key]
}

// LookupOverlapped reports overlap only when both lookups use the same key.
func (p *Provider) LookupOverlapped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lookupOverlap
}
