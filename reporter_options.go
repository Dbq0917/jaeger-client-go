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
	"time"

	"github.com/uber/jaeger-client-go/log"
)

// ReporterOption is a function that sets some option on the reporter.
type ReporterOption func(c *reporterOptions)

// ReporterOptions is a factory for all available ReporterOption's
var ReporterOptions reporterOptions

// reporterOptions control behavior of the reporter.
type reporterOptions struct {
	// queueSize is the size of internal queue where reported spans are stored before they are processed in the background
	queueSize int
	// bufferFlushInterval is how often the buffer is force-flushed, even if it's not full
	bufferFlushInterval time.Duration
	// logger is used to log errors of span submissions
	logger log.DebugLogger
	// metrics is used to record runtime stats
	metrics *Metrics
	// retryQueueSize is the maximum number of failed batches kept for replay.
	// Once the limit is reached the oldest failed batches are dropped.
	retryQueueSize int
	// retryInitialInterval is the delay before the first replay of a failed batch.
	retryInitialInterval time.Duration
	// retryMaxInterval caps the exponential backoff between replay attempts.
	retryMaxInterval time.Duration
	// retryReplayInterval paces how often queued batches are replayed after the
	// backend recovers, so that a large backlog cannot flood the server.
	retryReplayInterval time.Duration
}

// QueueSize creates a ReporterOption that sets the size of the internal queue where
// spans are stored before they are processed.
func (reporterOptions) QueueSize(queueSize int) ReporterOption {
	return func(r *reporterOptions) {
		r.queueSize = queueSize
	}
}

// Metrics creates a ReporterOption that initializes Metrics in the reporter,
// which is used to record runtime statistics.
func (reporterOptions) Metrics(metrics *Metrics) ReporterOption {
	return func(r *reporterOptions) {
		r.metrics = metrics
	}
}

// BufferFlushInterval creates a ReporterOption that sets how often the queue
// is force-flushed.
func (reporterOptions) BufferFlushInterval(bufferFlushInterval time.Duration) ReporterOption {
	return func(r *reporterOptions) {
		r.bufferFlushInterval = bufferFlushInterval
	}
}

// RetryQueueSize creates a ReporterOption that sets the capacity of the retry
// queue, i.e. the maximum number of failed batches retained for replay. When
// the capacity is reached the oldest batches are dropped (and counted in the
// reporter_retry_spans{result=dropped} metric) instead of growing memory.
func (reporterOptions) RetryQueueSize(retryQueueSize int) ReporterOption {
	return func(r *reporterOptions) {
		r.retryQueueSize = retryQueueSize
	}
}

// RetryInitialInterval creates a ReporterOption that sets the delay before the
// first replay attempt of a failed flush.
func (reporterOptions) RetryInitialInterval(retryInitialInterval time.Duration) ReporterOption {
	return func(r *reporterOptions) {
		r.retryInitialInterval = retryInitialInterval
	}
}

// RetryMaxInterval creates a ReporterOption that sets the upper bound of the
// exponential backoff applied to repeated replay failures.
func (reporterOptions) RetryMaxInterval(retryMaxInterval time.Duration) ReporterOption {
	return func(r *reporterOptions) {
		r.retryMaxInterval = retryMaxInterval
	}
}

// RetryReplayInterval creates a ReporterOption that sets the minimum interval
// between replay attempts once the backend is healthy again, bounding the
// replay rate of a large backlog.
func (reporterOptions) RetryReplayInterval(retryReplayInterval time.Duration) ReporterOption {
	return func(r *reporterOptions) {
		r.retryReplayInterval = retryReplayInterval
	}
}

// Logger creates a ReporterOption that initializes the logger used to log
// errors of span submissions.
func (reporterOptions) Logger(logger Logger) ReporterOption {
	return func(r *reporterOptions) {
		r.logger = log.DebugLogAdapter(logger)
	}
}
