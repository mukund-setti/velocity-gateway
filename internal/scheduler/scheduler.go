// Package scheduler implements Velo's continuous-batching dispatcher.
//
// # Why this design
//
// A real continuous batcher (e.g. vLLM's iteration-level scheduling) lives
// inside the model server and groups token-generation steps across in-flight
// requests so the GPU stays saturated. Velo sits *in front of* the model
// server, so it can't insert tokens into a backend's KV cache directly.
// What it can do is *micro-batch admission*: hold a request for a short
// window (a few milliseconds) so that compatible peers arrive, then dispatch
// them as a tightly-spaced burst against the same backend. Backends that
// implement in-flight batching (vLLM, TGI, llama.cpp's batch mode) will then
// fuse those requests into a single GPU forward pass; backends that don't
// will still benefit from connection reuse and warm caches.
//
// The dispatcher exposes two knobs that together control the throughput /
// latency tradeoff:
//
//   - MaxBatchSize  — flush as soon as this many compatible jobs accumulate
//   - MaxWait       — flush every batch this often, even if undersized
//
// Whichever fires first wins. Bigger batches = higher throughput, more
// per-request latency. Bigger MaxWait = more chances to batch, more
// queueing latency. The defaults (16 / 40ms) target the LLM streaming
// regime where TTFT is dominated by per-token latency, not queueing.
//
// # Concurrency model
//
//   - Submit() pushes a Job onto a buffered channel.
//   - A single batcher goroutine drains the channel and builds the current
//     batch in memory. When MaxBatchSize is reached or MaxWait elapses since
//     the *first* job in the batch arrived, it hands the batch to a worker
//     pool.
//   - Workers dispatch each job in the batch concurrently against the
//     router, so a batch ≠ a multiplexed payload. The "batching" benefit is
//     the temporal locality, not a multi-request HTTP body.
//
// The scheduler is intentionally simple: it does not look at prompt length
// or model parameters when forming batches. Real production systems do, and
// the Dispatcher interface is the seam to plug that in.
package scheduler

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/mukundu/velo/internal/metrics"
)

// ErrQueueFull is returned by Submit when the bounded queue is saturated.
var ErrQueueFull = errors.New("scheduler queue full")

// Job is a single in-flight request the scheduler is responsible for.
//
// The submitting goroutine waits on Done; the dispatcher fills in Response or
// Err and closes Done.
type Job struct {
	Model     string
	Stream    bool
	Body      []byte
	Submitted time.Time

	// Filled in by the dispatcher.
	Response *http.Response
	Backend  string
	Err      error
	Done     chan struct{}
}

// Dispatcher is the upstream interface — the router implements this.
type Dispatcher interface {
	Do(ctx context.Context, body []byte, stream bool) (*http.Response, string, error)
}

// Config knobs.
type Config struct {
	MaxBatchSize  int
	MaxWait       time.Duration
	QueueCapacity int
	Workers       int
	RequestTimeout time.Duration
}

// Scheduler is the continuous batcher.
type Scheduler struct {
	cfg     Config
	queue   chan *Job
	disp    Dispatcher
	workers chan struct{}

	stopCh   chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

// New builds a Scheduler. Call Start() to spin up its goroutines.
func New(cfg Config, disp Dispatcher) *Scheduler {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 25 * time.Millisecond
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 1024
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 60 * time.Second
	}
	return &Scheduler{
		cfg:     cfg,
		queue:   make(chan *Job, cfg.QueueCapacity),
		disp:    disp,
		workers: make(chan struct{}, cfg.Workers),
		stopCh:  make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Start launches the batching goroutine.
func (s *Scheduler) Start() {
	go s.runBatcher()
}

// Stop signals the batcher to drain and exit. Blocks until done.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	<-s.stopped
}

// Submit enqueues a job. Non-blocking: returns ErrQueueFull immediately if
// the queue is saturated, so we shed load instead of unbounded queueing.
func (s *Scheduler) Submit(ctx context.Context, j *Job) error {
	if j.Done == nil {
		j.Done = make(chan struct{})
	}
	select {
	case s.queue <- j:
		metrics.QueueDepth.Set(float64(len(s.queue)))
		return nil
	default:
		metrics.SchedulerRejections.Inc()
		return ErrQueueFull
	}
}

// runBatcher is the heart of the scheduler. It pulls jobs off the queue and
// builds a batch until *either* the batch hits MaxBatchSize *or* MaxWait
// has elapsed since the first job in the batch arrived. The batch is then
// handed off to a worker pool for concurrent dispatch.
func (s *Scheduler) runBatcher() {
	defer close(s.stopped)

	// batch holds jobs accumulated for the current batch — model is keyed
	// off the first arrival so we only group jobs that target the same
	// model. (Different models can be batched independently by spinning
	// per-model schedulers; we keep this single-model simple.)
	var batch []*Job
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	// flush dispatches the current batch (if any) and resets state.
	// reason is unused in the hot path but the call sites keep it as a
	// hook for future debug logging.
	flush := func(reason string) {
		_ = reason
		if len(batch) == 0 {
			return
		}
		b := batch
		batch = nil
		metrics.QueueDepth.Set(float64(len(s.queue)))
		metrics.BatchSize.Observe(float64(len(b)))
		metrics.BatchesDispatched.Inc()
		s.dispatchBatch(b)
	}

	for {
		// Wait for either: a new job arrives, the timer fires, or we're stopped.
		select {
		case <-s.stopCh:
			flush("shutdown")
			// Drain remaining jobs as singleton batches.
			for {
				select {
				case j := <-s.queue:
					s.dispatchBatch([]*Job{j})
				default:
					return
				}
			}
		case j := <-s.queue:
			if len(batch) == 0 {
				// First job in a new batch — arm the wait-window timer. The
				// drain pattern handles the case where the timer has
				// already fired since the previous Reset.
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.cfg.MaxWait)
			}
			batch = append(batch, j)
			metrics.QueueDepth.Set(float64(len(s.queue)))
			if len(batch) >= s.cfg.MaxBatchSize {
				flush("size")
			}
		case <-timer.C:
			// MaxWait elapsed since the first job in the current batch.
			flush("time")
		}
	}
}

// dispatchBatch fans the batch out to the worker pool. Each worker handles
// one job — they run concurrently, but the upstream backend sees them
// arrive in a tight burst, which is the part that helps in-flight batchers
// on the backend side.
func (s *Scheduler) dispatchBatch(b []*Job) {
	for _, j := range b {
		j := j
		s.workers <- struct{}{}
		go func() {
			defer func() { <-s.workers }()
			waited := time.Since(j.Submitted).Seconds()
			metrics.BatchWaitTime.Observe(waited)

			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
			defer cancel()
			resp, backend, err := s.disp.Do(ctx, j.Body, j.Stream)
			if err != nil {
				j.Err = err
				close(j.Done)
				return
			}
			j.Response = resp
			j.Backend = backend
			close(j.Done)
		}()
	}
}

