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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRetryOptions() reporterOptions {
	return reporterOptions{
		retryQueueSize:       3,
		retryInitialInterval: 30 * time.Second,
		retryMaxInterval:     2 * time.Minute,
		retryReplayInterval:  100 * time.Millisecond,
	}
}

func newTestRetryQueue() *retryQueue {
	return newRetryQueue(testRetryOptions())
}

func TestRetryQueueBoundedDropsOldest(t *testing.T) {
	q := newTestRetryQueue()

	// Fill the queue to capacity with one span per batch.
	for i := 0; i < 3; i++ {
		span := &Span{}
		span.Retain() // queue takes ownership of one reference
		dropped := q.enqueue([]*Span{span})
		require.Equal(t, 0, dropped)
	}
	require.Equal(t, 3, q.len())

	// A fourth batch must evict the oldest instead of growing without bound.
	overflow := &Span{}
	overflow.Retain()
	dropped := q.enqueue([]*Span{overflow})
	assert.Equal(t, 1, dropped, "oldest batch (1 span) should be dropped")
	assert.Equal(t, 3, q.len(), "queue length must never exceed capacity")

	// Multi-span batches count all dropped spans.
	big := []*Span{{}, {}}
	for _, s := range big {
		s.Retain()
	}
	dropped = q.enqueue(big)
	assert.Equal(t, 1, dropped)
	assert.Equal(t, 3, q.len())

	// Releasing everything still held must not panic (dropped spans were
	// released by the queue, the rest are released here).
	q.releaseAll()
	assert.Equal(t, 0, q.len())
}

func TestRetryQueueExponentialBackoff(t *testing.T) {
	q := newTestRetryQueue()
	now := time.Unix(0, 0)

	span := &Span{}
	span.Retain()
	require.Equal(t, 0, q.enqueue([]*Span{span}))
	q.armBackoff(now)
	assert.False(t, q.ready(now), "must wait for the initial interval before the first replay")

	expected := []time.Duration{
		30 * time.Second,
		60 * time.Second,
		120 * time.Second,
		120 * time.Second, // capped at max interval
	}
	current := now
	for i, want := range expected {
		// Not ready before the delay elapses.
		assert.False(t, q.ready(current.Add(want-time.Second)), "attempt %d must not be early", i+1)
		current = current.Add(want)
		require.True(t, q.ready(current), "attempt %d must be ready after %s", i+1, want)

		batch := q.pop()
		require.Len(t, batch.spans, 1)
		q.onFailure(current) // replay fails again

		// The batch stays queued and is not ready immediately.
		assert.Equal(t, 1, q.len())
		assert.False(t, q.ready(current))
	}
	q.releaseAll()
}

func TestRetryQueueBackoffResetOnSuccess(t *testing.T) {
	q := newTestRetryQueue()
	now := time.Unix(0, 0)

	first := &Span{}
	first.Retain()
	q.enqueue([]*Span{first})
	q.armBackoff(now)

	// Fail once so the backoff grows.
	firstAttemptAt := now.Add(30 * time.Second)
	require.True(t, q.ready(firstAttemptAt))
	q.pop()
	q.onFailure(firstAttemptAt)
	assert.Equal(t, 60*time.Second, q.currentDelay)

	// A successful replay resets the delay back to the initial interval.
	secondAttemptAt := firstAttemptAt.Add(60 * time.Second)
	require.True(t, q.ready(secondAttemptAt))
	q.pop()
	q.onSuccess(secondAttemptAt)
	assert.Equal(t, 0, q.len())
	assert.Equal(t, 30*time.Second, q.currentDelay)
	// Draining the last batch resets pacing state for the next outage.
	q.resetPacing()
	assert.True(t, q.nextAttempt.IsZero())
}

func TestRetryQueueReadyIsFalseWhenEmpty(t *testing.T) {
	q := newTestRetryQueue()
	assert.False(t, q.ready(time.Now()))
}
