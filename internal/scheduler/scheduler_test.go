package scheduler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeDispatcher records the batches it observes.
//
// Because the scheduler dispatches each job in a batch concurrently, we
// can't observe "a batch" directly — but we can capture the timestamps each
// job arrived at and reconstruct batches by looking at clusters in arrival
// time. We use a small grace window for clustering.
type fakeDispatcher struct {
	mu       sync.Mutex
	calls    []time.Time
	delay    time.Duration
	errEvery int64
	count    atomic.Int64
}

func (f *fakeDispatcher) Do(ctx context.Context, body []byte, stream bool) (*http.Response, string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, time.Now())
	f.mu.Unlock()
	n := f.count.Add(1)
	if f.errEvery > 0 && n%f.errEvery == 0 {
		return nil, "", errors.New("boom")
	}
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	body0 := io.NopCloser(bytes.NewReader([]byte("ok")))
	return &http.Response{StatusCode: 200, Body: body0}, "fake", nil
}

// TestScheduler_FlushOnMaxBatchSize verifies that the scheduler flushes a
// batch the moment MaxBatchSize compatible jobs arrive, *without* waiting
// for MaxWait to elapse.
func TestScheduler_FlushOnMaxBatchSize(t *testing.T) {
	disp := &fakeDispatcher{}
	s := New(Config{
		MaxBatchSize: 4,
		MaxWait:      500 * time.Millisecond, // intentionally large
		QueueCapacity: 64,
		Workers:      8,
		RequestTimeout: time.Second,
	}, disp)
	s.Start()
	defer s.Stop()

	start := time.Now()
	var jobs []*Job
	for i := 0; i < 4; i++ {
		j := &Job{Body: []byte("x"), Submitted: time.Now(), Done: make(chan struct{})}
		require.NoError(t, s.Submit(context.Background(), j))
		jobs = append(jobs, j)
	}
	for _, j := range jobs {
		<-j.Done
		require.NoError(t, j.Err)
	}
	elapsed := time.Since(start)
	// Must flush well before MaxWait — if we waited for the timer the test
	// would take at least ~500ms.
	require.Less(t, elapsed, 200*time.Millisecond, "scheduler should flush on size, not wait for timer")
	require.Equal(t, int64(4), disp.count.Load())
}

// TestScheduler_FlushOnTimeout verifies that an under-sized batch still
// flushes after MaxWait so single requests don't stall.
func TestScheduler_FlushOnTimeout(t *testing.T) {
	disp := &fakeDispatcher{}
	s := New(Config{
		MaxBatchSize: 16,
		MaxWait:      30 * time.Millisecond,
		QueueCapacity: 64,
		Workers:      4,
		RequestTimeout: time.Second,
	}, disp)
	s.Start()
	defer s.Stop()

	j := &Job{Body: []byte("x"), Submitted: time.Now(), Done: make(chan struct{})}
	require.NoError(t, s.Submit(context.Background(), j))

	start := time.Now()
	select {
	case <-j.Done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("job never dispatched")
	}
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "should wait at least most of MaxWait")
	require.Less(t, elapsed, 200*time.Millisecond, "should not stall waiting for non-arriving jobs")
}

// TestScheduler_QueueFull verifies load-shedding when the bounded queue
// saturates.
func TestScheduler_QueueFull(t *testing.T) {
	disp := &fakeDispatcher{delay: 200 * time.Millisecond}
	s := New(Config{
		MaxBatchSize:   1,
		MaxWait:        1 * time.Millisecond,
		QueueCapacity:  2,
		Workers:        1,
		RequestTimeout: time.Second,
	}, disp)
	s.Start()
	defer s.Stop()

	// Saturate.
	rejected := 0
	for i := 0; i < 100; i++ {
		j := &Job{Body: []byte("x"), Submitted: time.Now(), Done: make(chan struct{})}
		if err := s.Submit(context.Background(), j); errors.Is(err, ErrQueueFull) {
			rejected++
		}
	}
	require.Greater(t, rejected, 0, "expected at least one ErrQueueFull")
}

// TestScheduler_DispatcherError surfaces the upstream error through Job.Err.
func TestScheduler_DispatcherError(t *testing.T) {
	disp := &fakeDispatcher{errEvery: 1}
	s := New(Config{
		MaxBatchSize: 1, MaxWait: 1 * time.Millisecond,
		QueueCapacity: 4, Workers: 2, RequestTimeout: time.Second,
	}, disp)
	s.Start()
	defer s.Stop()

	j := &Job{Body: []byte("x"), Submitted: time.Now(), Done: make(chan struct{})}
	require.NoError(t, s.Submit(context.Background(), j))
	<-j.Done
	require.Error(t, j.Err)
}
