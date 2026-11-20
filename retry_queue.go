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
	"sync"
	"time"
)

const (
	// defaultRetryQueueSize bounds the number of failed batches kept in memory.
	defaultRetryQueueSize = 1000
	// defaultRetryInitialInterval is the wait before the first replay attempt.
	defaultRetryInitialInterval = 30 * time.Second
	// defaultRetryMaxInterval caps the exponential backoff so that retries do
	// not stop forever during a long outage.
	defaultRetryMaxInterval = 5 * time.Minute
	// defaultRetryReplayInterval paces how often a batch is replayed after the
	// backend recovers, preventing a recovered server from being flooded.
	defaultRetryReplayInterval = time.Second
)

// retryBatch is a group of spans whose flush failed and that must be replayed.
// The queue owns one reference per span: it is transferred in by enqueue and
// released when the batch is sent successfully, dropped, or on shutdown.
type retryBatch struct {
	spans []*Span
}

// retryQueue holds spans that failed to be reported. Unlike the main reporter
// queue it is explicitly bounded: when the capacity is reached the oldest
// batches are dropped instead of letting the process grow without limit.
// All methods are expected to be called from the reporter's single
// processQueue goroutine, except Length() which can be read concurrently.
type retryQueue struct {
	mu sync.Mutex

	options reporterOptions

	items []retryBatch

	// backoff state
	nextAttempt  time.Time
	currentDelay time.Duration
}

func newRetryQueue(options reporterOptions) *retryQueue {
	return &retryQueue{
		options:      options,
		items:        make([]retryBatch, 0, options.retryQueueSize),
		currentDelay: options.retryInitialInterval,
	}
}

// enqueue stores a failed batch for later replay. Reference ownership of the
// spans is transferred to the queue, so the caller must not release them. If
// the queue is full the oldest batches (and their spans) are dropped to make
// room; the number of dropped spans is returned.
func (q *retryQueue) enqueue(spans []*Span) (dropped int) {
	if len(spans) == 0 {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	capacity := q.options.retryQueueSize
	for len(q.items) >= capacity {
		dropped += q.dropOldest()
	}
	q.items = append(q.items, retryBatch{spans: spans})
	return dropped
}

// dropOldest removes and releases the batch that has been in the queue the
// longest, returning the number of spans it contained.
func (q *retryQueue) dropOldest() int {
	if len(q.items) == 0 {
		return 0
	}
	oldest := q.items[0]
	q.items = q.items[1:]
	for _, span := range oldest.spans {
		span.Release()
	}
	return len(oldest.spans)
}

// len returns the number of batches currently waiting to be replayed.
func (q *retryQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// spanCount returns the total number of spans held across all queued batches.
func (q *retryQueue) spanCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	total := 0
	for _, item := range q.items {
		total += len(item.spans)
	}
	return total
}

// ready reports whether the backoff delay has elapsed at time now, i.e. the
// head batch is allowed to be replayed. Replay pacing after recovery is driven
// by the timer re-armed with replayInterval in processRetries.
func (q *retryQueue) ready(now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return false
	}
	return !now.Before(q.nextAttempt)
}

// pop returns the oldest batch for replay, leaving it in the queue until the
// caller reports the outcome via onSuccess/onFailure.
func (q *retryQueue) pop() retryBatch {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.items[0]
}

// onSuccess removes the head batch after a successful replay, releases its
// spans, resets the exponential backoff and arms the paced replay mode so that
// the remaining backlog is drained at replayInterval cadence. now is the time
// of the successful replay, used to schedule the next paced attempt.
func (q *retryQueue) onSuccess(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return
	}
	head := q.items[0]
	q.items = q.items[1:]
	for _, span := range head.spans {
		span.Release()
	}
	q.currentDelay = q.options.retryInitialInterval
	// Schedule the next attempt strictly one replay interval from now so the
	// backlog is drained at a controlled, roughly constant rate.
	q.nextAttempt = now.Add(q.options.retryReplayInterval)
}

// onFailure re-arms the exponential backoff after a failed replay and keeps
// the head batch in the queue for the next attempt.
func (q *retryQueue) onFailure(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Grow the backoff first, then schedule the next attempt, so that the
	// delays form a clean geometric sequence: initial, 2*initial, 4*initial, ...
	if next := q.currentDelay * 2; next <= q.options.retryMaxInterval {
		q.currentDelay = next
	} else {
		q.currentDelay = q.options.retryMaxInterval
	}
	q.nextAttempt = now.Add(q.currentDelay)
}

// armBackoff schedules the next retry attempt. It is called whenever a failed
// batch is added while no attempt is pending, so the delay always reflects the
// current exponential backoff rather than a stale timestamp.
func (q *retryQueue) armBackoff(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextAttempt = now.Add(q.currentDelay)
}

// resetPacing clears the recovery-mode pacing flag once the queue is drained.
func (q *retryQueue) resetPacing() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextAttempt = time.Time{}
}

// releaseAll releases every held span; used when the reporter is closed.
func (q *retryQueue) releaseAll() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range q.items {
		for _, span := range item.spans {
			span.Release()
		}
	}
	q.items = q.items[:0]
}
