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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingSender stalls inside Flush until released, emulating a transport whose
// network call never returns during shutdown. Append routes through Flush once
// the internal buffer fills, mirroring how the real UDP sender behaves.
type blockingSender struct {
	mu         sync.Mutex
	bufferSize int
	buffered   []*Span
	flushed    []*Span

	enterFlush chan struct{}
	release    chan struct{}
	once       sync.Once
}

func newBlockingSender(bufferSize int) *blockingSender {
	return &blockingSender{
		bufferSize: bufferSize,
		enterFlush: make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (s *blockingSender) Append(span *Span) (int, error) {
	s.mu.Lock()
	s.buffered = append(s.buffered, span)
	if len(s.buffered) < s.bufferSize {
		s.mu.Unlock()
		return 0, nil
	}
	spans := s.buffered
	s.buffered = nil
	s.mu.Unlock()
	return s.flush(spans)
}

func (s *blockingSender) Flush() (int, error) {
	s.mu.Lock()
	spans := s.buffered
	s.buffered = nil
	s.mu.Unlock()
	return s.flush(spans)
}

// flush parks the worker until unblock() is called, then records the spans as
// successfully delivered.
func (s *blockingSender) flush(spans []*Span) (int, error) {
	if len(spans) == 0 {
		return 0, nil
	}
	s.once.Do(func() { close(s.enterFlush) })
	<-s.release
	s.mu.Lock()
	s.flushed = append(s.flushed, spans...)
	n := len(spans)
	s.mu.Unlock()
	return n, nil
}

func (s *blockingSender) unblock() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

func (s *blockingSender) Close() error { return nil }

func (s *blockingSender) flushedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.flushed)
}

func (s *blockingSender) bufferedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buffered)
}

// memorySpiller records the spans handed to it, optionally failing after a
// configurable number of successful spills.
type memorySpiller struct {
	mu        sync.Mutex
	spans     []*Span
	failAfter int
	err       error
}

func (m *memorySpiller) Spill(spans []*Span) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	saved := 0
	for _, span := range spans {
		if m.failAfter > 0 && len(m.spans) >= m.failAfter {
			return saved, m.err
		}
		m.spans = append(m.spans, span)
		saved++
	}
	if m.err != nil && m.failAfter == 0 {
		return 0, m.err
	}
	return saved, nil
}

func (m *memorySpiller) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.spans)
}

func newShutdownSuite(t *testing.T, sender Transport, spiller SpanSpiller, timeout time.Duration, opts ...ReporterOption) *reporterSuite {
	allOpts := append([]ReporterOption{
		ReporterOptions.ShutdownTimeout(timeout),
		ReporterOptions.SpillHandler(spiller),
	}, opts...)
	return makeReporterSuiteWithSender(t, sender, allOpts...)
}

// TestRemoteReporterCloseFlushesWithinTimeout verifies that when the backend
// keeps responding, Close() drains every queued span well within the timeout
// and never touches the spiller.
func TestRemoteReporterCloseFlushesWithinTimeout(t *testing.T) {
	spiller := &memorySpiller{}
	s := newShutdownSuite(t, &fakeSender{bufferSize: 1}, spiller, time.Second, ReporterOptions.QueueSize(600))

	const total = 500
	for i := 0; i < total; i++ {
		s.tracer.StartSpan("op").Finish()
	}

	start := time.Now()
	s.close()
	elapsed := time.Since(start)

	assert.True(t, elapsed < time.Second, "close should return within the shutdown timeout, took %v", elapsed)
	s.sender.(*fakeSender).assertFlushedSpans(t, total)
	assert.Equal(t, 0, spiller.count(), "no spans should be spilled when flushing succeeds")
}

// TestRemoteReporterCloseSpillsOnStuckFlush reproduces the rolling-restart
// scenario: the final flush never returns, so Close() must hit its timeout and
// persist the spans still waiting in the queue.
func TestRemoteReporterCloseSpillsOnStuckFlush(t *testing.T) {
	// A buffer of 1 makes the first Append trigger a flush, where the worker parks.
	sender := newBlockingSender(1)
	spiller := &memorySpiller{}
	s := newShutdownSuite(t, sender, spiller, 100*time.Millisecond, ReporterOptions.QueueSize(10))

	const total = 50
	for i := 0; i < total; i++ {
		s.tracer.StartSpan("op").Finish()
	}

	select {
	case <-sender.enterFlush:
	case <-time.After(time.Second):
		t.Fatal("worker never entered the blocking flush")
	}

	start := time.Now()
	s.close()
	elapsed := time.Since(start)

	assert.True(t, elapsed >= 100*time.Millisecond, "close should wait for the timeout, took %v", elapsed)
	assert.True(t, elapsed < time.Second, "Close must be bounded by the shutdown timeout, took %v", elapsed)
	assert.Greater(t, spiller.count(), 0, "queued spans must be spilled instead of being lost")

	sender.unblock() // release the parked worker so the test process can exit
}

// TestRemoteReporterCloseWhileReporting ensures spans produced concurrently with
// shutdown are accounted for: flushed, buffered in the sender, or spilled.
func TestRemoteReporterCloseWhileReporting(t *testing.T) {
	sender := newBlockingSender(1)
	spiller := &memorySpiller{}
	s := newShutdownSuite(t, sender, spiller, 150*time.Millisecond, ReporterOptions.QueueSize(50))

	const total = 200
	var producers sync.WaitGroup
	for i := 0; i < 4; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < total/4; j++ {
				s.tracer.StartSpan("op").Finish()
			}
		}()
	}
	producers.Wait()

	select {
	case <-sender.enterFlush:
	case <-time.After(time.Second):
	}

	s.close()

	accounted := sender.flushedCount() + sender.bufferedCount() + spiller.count()
	assert.LessOrEqual(t, accounted, total)
	assert.Greater(t, accounted, 0, "every produced span should be flushed, buffered, or spilled")

	sender.unblock()
}

// TestRemoteReporterSpillFailureCountsDropped verifies that spans the spiller
// cannot persist are reported as dropped rather than vanishing quietly.
func TestRemoteReporterSpillFailureCountsDropped(t *testing.T) {
	const queueSize = 20
	sender := newBlockingSender(1) // first Append parks inside Flush
	spiller := &memorySpiller{failAfter: 1, err: errors.New("disk full")}
	s := newShutdownSuite(t, sender, spiller, 80*time.Millisecond, ReporterOptions.QueueSize(queueSize))

	// Fill the queue without overflowing: the worker appends the first span then
	// blocks in Flush, leaving the other queueSize-1 spans waiting in the queue.
	for i := 0; i < queueSize; i++ {
		s.tracer.StartSpan("op").Finish()
	}

	select {
	case <-sender.enterFlush:
	case <-time.After(time.Second):
		t.Fatal("worker never entered the blocking flush")
	}

	s.close()
	sender.unblock()

	// queueSize-1 spans were spill candidates; only the first one is saved, the
	// rest must be counted as dropped.
	assert.Equal(t, 1, spiller.count(), "only the first spill should succeed")
	s.assertCounter(t, "jaeger.tracer.reporter_spans", map[string]string{"result": "dropped"}, int64(queueSize-2))
}

// TestRemoteReporterNegativeTimeoutWaitsForDrain preserves the historical
// behavior: a negative shutdown timeout means Close() waits as long as needed
// for the queue to fully drain rather than spilling.
func TestRemoteReporterNegativeTimeoutWaitsForDrain(t *testing.T) {
	sender := newBlockingSender(1)
	spiller := &memorySpiller{}
	s := newShutdownSuite(t, sender, spiller, -1, ReporterOptions.QueueSize(10))

	for i := 0; i < 20; i++ {
		s.tracer.StartSpan("op").Finish()
	}

	select {
	case <-sender.enterFlush:
	case <-time.After(time.Second):
		t.Fatal("worker never entered the blocking flush")
	}

	done := make(chan struct{})
	go func() {
		s.close()
		close(done)
	}()

	// With an unbounded timeout Close must still be blocked while the worker is stuck.
	select {
	case <-done:
		t.Fatal("Close returned before the stuck flush was released")
	case <-time.After(50 * time.Millisecond):
	}

	sender.unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the worker unblocked")
	}
	assert.Equal(t, 0, spiller.count(), "unbounded shutdown must flush, never spill")
}

// TestRemoteReporterDefaultShutdownTimeout verifies the reporter applies a
// bounded default timeout even when none is configured.
func TestRemoteReporterDefaultShutdownTimeout(t *testing.T) {
	reporter := NewRemoteReporter(newBlockingSender(1)).(*remoteReporter)
	assert.Equal(t, defaultShutdownTimeout, reporter.shutdownTimeout)
	require.NoError(t, reporter.sender.Close())
}
