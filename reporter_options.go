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
	// shutdownTimeout bounds how long Close() waits for the background worker to drain
	// the queue and flush the buffered spans before falling back to spilling the
	// remaining spans to disk. A negative value means wait indefinitely.
	shutdownTimeout time.Duration
	// spiller persists spans that could not be flushed before shutdownTimeout elapses.
	spiller SpanSpiller
	// logger is used to log errors of span submissions
	logger log.DebugLogger
	// metrics is used to record runtime stats
	metrics *Metrics
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

// ShutdownTimeout creates a ReporterOption that bounds the time Close() spends
// trying to flush buffered spans. When the timeout elapses before all spans are
// sent, any spans still held by the reporter are handed to the configured
// SpanSpiller (see SpillHandler) so that they are not lost. A negative duration
// disables the bound and makes Close() wait until the queue is fully drained.
func (reporterOptions) ShutdownTimeout(shutdownTimeout time.Duration) ReporterOption {
	return func(r *reporterOptions) {
		r.shutdownTimeout = shutdownTimeout
	}
}

// SpillHandler creates a ReporterOption that sets the handler used to persist
// spans that remain unsent when Close() reaches its shutdown timeout.
func (reporterOptions) SpillHandler(spiller SpanSpiller) ReporterOption {
	return func(r *reporterOptions) {
		r.spiller = spiller
	}
}

// Logger creates a ReporterOption that initializes the logger used to log
// errors of span submissions.
func (reporterOptions) Logger(logger Logger) ReporterOption {
	return func(r *reporterOptions) {
		r.logger = log.DebugLogAdapter(logger)
	}
}
