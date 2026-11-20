// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jaeger

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/metricstest"
)

// controllableSender simulates a backend whose availability can be toggled.
// While unavailable every flush fails; once recovered flushes succeed and the
// received spans are recorded with timestamps so replay rate can be measured.
type controllableSender struct {
	mu sync.Mutex

	bufferSize int
	available  bool

	buffered []*Span
	// received records every successfully delivered span and when it arrived.
	received []timedSpan
	// flushTimes records the timestamp of every successful flush call.
	flushTimes []time.Time
}

type timedSpan struct {
	span *Span
	at   time.Time
}

func (s *controllableSender) setAvailable(available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = available
}

func (s *controllableSender) Append(span *Span) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buffered = append(s.buffered, span)
	if len(s.buffered) < s.bufferSize {
		return 0, nil
	}
	return s.flushNoLock()
}

func (s *controllableSender) Flush() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushNoLock()
}

func (s *controllableSender) flushNoLock() (int, error) {
	n := len(s.buffered)
	if n == 0 {
		return 0, nil
	}
	if !s.available {
		// Mimic a real transport: the buffer is cleared on a failed emit and the
		// spans must be retried by the reporter.
		s.buffered = s.buffered[:0]
		return n, errors.New("backend unavailable")
	}
	now := time.Now()
	for _, span := range s.buffered {
		s.received = append(s.received, timedSpan{span: span, at: now})
	}
	s.flushTimes = append(s.flushTimes, now)
	s.buffered = s.buffered[:0]
	return n, nil
}

func (s *controllableSender) Close() error { return nil }

func (s *controllableSender) snapshot() ([]timedSpan, []time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	received := make([]timedSpan, len(s.received))
	copy(received, s.received)
	flushes := make([]time.Time, len(s.flushTimes))
	copy(flushes, s.flushTimes)
	return received, flushes
}

func (s *controllableSender) bufferedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buffered)
}

// TestRemoteReporterNormalReportUnaffectedByRetryBacklog verifies that a
// saturated retry backlog does not block or slow down normal reporting:
// Report() must keep returning immediately because the live reporter queue and
// the retry queue use separate, independently bounded memory.
func TestRemoteReporterNormalReportUnaffectedByRetryBacklog(t *testing.T) {
	const retryQueueSize = 5
	sender := &controllableSender{bufferSize: 2, available: false}
	reporter, _, tracer, closer := newRetryTestReporter(t, sender, retryQueueSize)
	defer closer.Close()

	// Saturate the retry queue during the outage.
	for i := 0; i < 50; i++ {
		tracer.StartSpan("op").Finish()
	}
	require.Eventually(t, func() bool {
		return reporter.retries.len() >= retryQueueSize
	}, time.Second, 5*time.Millisecond)

	// Even with a full retry backlog and a still-down backend, freshly reported
	// spans must be accepted into the main queue without blocking. A blocked
	// Report() would blow this generous deadline.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			tracer.StartSpan("op").Finish()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("normal reporting blocked while retry backlog is saturated")
	}

	// The retry backlog stays bounded regardless of how much more is reported.
	assert.LessOrEqual(t, reporter.retries.len(), retryQueueSize)
}

// newRetryTestReporter builds a remote reporter wired to a controllable
// sender with short, deterministic retry timings.
func newRetryTestReporter(t *testing.T, sender *controllableSender, retryQueueSize int) (*remoteReporter, *metricstest.Factory, opentracing.Tracer, io.Closer) {
	metricsFactory := metricstest.NewFactory(0)
	metrics := NewMetrics(metricsFactory, nil)
	reporter := NewRemoteReporter(sender,
		ReporterOptions.Metrics(metrics),
		ReporterOptions.QueueSize(2000),
		ReporterOptions.BufferFlushInterval(20*time.Millisecond),
		ReporterOptions.RetryQueueSize(retryQueueSize),
		ReporterOptions.RetryInitialInterval(40*time.Millisecond),
		ReporterOptions.RetryMaxInterval(80*time.Millisecond),
		ReporterOptions.RetryReplayInterval(30*time.Millisecond),
	).(*remoteReporter)
	tracer, closer := NewTracer("retry-test", NewConstSampler(true), reporter)
	require.NotNil(t, tracer)
	return reporter, metricsFactory, tracer, closer
}

// TestRemoteReporterRetryQueueBoundedDuringOutage reproduces a long backend
// outage (the "ten minutes" scenario, compressed in time) and asserts that the
// retry backlog never exceeds its capacity, so memory stays bounded instead of
// growing to hundreds of thousands of spans and OOM-killing the process.
func TestRemoteReporterRetryQueueBoundedDuringOutage(t *testing.T) {
	const (
		retryQueueSize = 20
		totalSpans     = 500 // far more than the queue could ever hold
	)
	sender := &controllableSender{bufferSize: 5, available: false}
	reporter, metricsFactory, tracer, closer := newRetryTestReporter(t, sender, retryQueueSize)
	defer closer.Close()

	for i := 0; i < totalSpans; i++ {
		tracer.StartSpan("op").Finish()
	}

	// Let all spans flow through the reporter while the backend stays down.
	require.Eventually(t, func() bool {
		return reporter.retries.spanCount() > 0
	}, time.Second, 5*time.Millisecond)

	// Keep observing the retry queue while retries keep failing: it must remain
	// bounded for the whole outage window.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		assert.LessOrEqual(t, reporter.retries.len(), retryQueueSize,
			"retry queue must never grow past its capacity")
		assert.LessOrEqual(t, reporter.retries.spanCount(), retryQueueSize*sender.bufferSize)
		time.Sleep(5 * time.Millisecond)
	}

	// Nothing could have been delivered while the backend was unavailable.
	received, _ := sender.snapshot()
	assert.Empty(t, received, "no spans should be delivered during the outage")

	// Overflow spans must be accounted for as drops rather than accumulated.
	counters, _ := metricsFactory.Snapshot()
	dropped := counters[metrics.GetKey("jaeger.tracer.reporter_retry_spans",
		map[string]string{"result": "dropped"}, "|", "=")]
	assert.Greater(t, dropped, int64(0), "retry overflow must be counted as drops")
}

// TestRemoteReporterReplayIsPacedAfterRecovery verifies that once the backend
// recovers, the accumulated backlog is drained at the configured replay
// interval instead of being blasted out in a single burst that re-crashes the
// server.
func TestRemoteReporterReplayIsPacedAfterRecovery(t *testing.T) {
	const (
		retryQueueSize = 40
		totalSpans     = 100
	)
	sender := &controllableSender{bufferSize: 5, available: false}
	reporter, metricsFactory, tracer, closer := newRetryTestReporter(t, sender, retryQueueSize)
	defer closer.Close()

	for i := 0; i < totalSpans; i++ {
		tracer.StartSpan("op").Finish()
	}

	// Wait until a bounded backlog has accumulated during the outage.
	require.Eventually(t, func() bool {
		return reporter.retries.spanCount() >= retryQueueSize
	}, time.Second, 5*time.Millisecond)

	// Recover the backend.
	sender.setAvailable(true)
	recoveryStart := time.Now()

	// All retained spans should eventually be replayed successfully. Wait for
	// the retry queue to drain and for flushing activity to settle, so the
	// snapshot below cannot be taken in the middle of the final replay.
	require.Eventually(t, func() bool {
		_, flushes := sender.snapshot()
		return reporter.retries.len() == 0 && len(flushes) >= 10
	}, 2*time.Second, 10*time.Millisecond)

	received, flushes := sender.snapshot()
	assert.NotEmpty(t, received)

	// The backlog must be delivered in many batches rather than a single
	// recovery burst. The sender auto-flushes every bufferSize spans, so the
	// number of flush events is a direct proxy for how many paced batches were
	// replayed, independent of internal timer timestamp granularity.
	require.Greater(t, len(flushes), 1, "replay must be split into paced batches")
	// With bufferSize=5 and ~99 replayed spans there should be close to 20
	// flush events, proving the data was streamed rather than dumped at once.
	assert.GreaterOrEqual(t, len(flushes), len(received)/(sender.bufferSize*2),
		"replay must be broken into a large number of small batches")
	// Pacing proof: ~20 replay batches at a 20ms interval cannot drain in one
	// burst. The total drain must take substantially longer than a single
	// replay interval; an unthrottled burst would finish in well under that.
	drainElapsed := flushes[len(flushes)-1].Sub(flushes[0])
	// Compare raw nanoseconds: testify v1.6 cannot compare named int types
	// (time.Duration) with its generic ordered-assertion helpers.
	assert.GreaterOrEqual(t, drainElapsed.Nanoseconds(), (100 * time.Millisecond).Nanoseconds(),
		"replay must be rate-limited over time, not sent in one burst")
	_ = recoveryStart

	// The retry success metric should reflect the replayed spans and the queue
	// gauge must be back to zero after draining.
	key := metrics.GetKey("jaeger.tracer.reporter_retry_spans",
		map[string]string{"result": "ok"}, "|", "=")
	require.Eventually(t, func() bool {
		counters, _ := metricsFactory.Snapshot()
		return counters[key] == int64(len(received))
	}, time.Second, 5*time.Millisecond)
	counters, _ := metricsFactory.Snapshot()
	assert.Equal(t, int64(len(received)), counters[key])
}
