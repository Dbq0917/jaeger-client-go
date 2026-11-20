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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/uber/jaeger-client-go/internal/reporterstats"
	"github.com/uber/jaeger-client-go/log"
)

// Reporter is called by the tracer when a span is completed to report the span to the tracing collector.
type Reporter interface {
	// Report submits a new span to collectors, possibly asynchronously and/or with buffering.
	// If the reporter is processing Span asynchronously then it needs to Retain() the span,
	// and then Release() it when no longer needed, to avoid span data corruption.
	Report(span *Span)

	// Close does a clean shutdown of the reporter, flushing any traces that may be buffered in memory.
	Close()
}

// ------------------------------

type nullReporter struct{}

// NewNullReporter creates a no-op reporter that ignores all reported spans.
func NewNullReporter() Reporter {
	return &nullReporter{}
}

// Report implements Report() method of Reporter by doing nothing.
func (r *nullReporter) Report(span *Span) {
	// no-op
}

// Close implements Close() method of Reporter by doing nothing.
func (r *nullReporter) Close() {
	// no-op
}

// ------------------------------

type loggingReporter struct {
	logger Logger
}

// NewLoggingReporter creates a reporter that logs all reported spans to provided logger.
func NewLoggingReporter(logger Logger) Reporter {
	return &loggingReporter{logger}
}

// Report implements Report() method of Reporter by logging the span to the logger.
func (r *loggingReporter) Report(span *Span) {
	r.logger.Infof("Reporting span %+v", span)
}

// Close implements Close() method of Reporter by doing nothing.
func (r *loggingReporter) Close() {
	// no-op
}

// ------------------------------

// InMemoryReporter is used for testing, and simply collects spans in memory.
type InMemoryReporter struct {
	spans []opentracing.Span
	lock  sync.Mutex
}

// NewInMemoryReporter creates a reporter that stores spans in memory.
// NOTE: the Tracer should be created with options.PoolSpans = false.
func NewInMemoryReporter() *InMemoryReporter {
	return &InMemoryReporter{
		spans: make([]opentracing.Span, 0, 10),
	}
}

// Report implements Report() method of Reporter by storing the span in the buffer.
func (r *InMemoryReporter) Report(span *Span) {
	r.lock.Lock()
	// Need to retain the span otherwise it will be released
	r.spans = append(r.spans, span.Retain())
	r.lock.Unlock()
}

// Close implements Close() method of Reporter
func (r *InMemoryReporter) Close() {
	r.Reset()
}

// SpansSubmitted returns the number of spans accumulated in the buffer.
func (r *InMemoryReporter) SpansSubmitted() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return len(r.spans)
}

// GetSpans returns accumulated spans as a copy of the buffer.
func (r *InMemoryReporter) GetSpans() []opentracing.Span {
	r.lock.Lock()
	defer r.lock.Unlock()
	copied := make([]opentracing.Span, len(r.spans))
	copy(copied, r.spans)
	return copied
}

// Reset clears all accumulated spans.
func (r *InMemoryReporter) Reset() {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Before reset the collection need to release Span memory
	for _, span := range r.spans {
		span.(*Span).Release()
	}
	r.spans = r.spans[:0]
}

// ------------------------------

type compositeReporter struct {
	reporters []Reporter
}

// NewCompositeReporter creates a reporter that ignores all reported spans.
func NewCompositeReporter(reporters ...Reporter) Reporter {
	return &compositeReporter{reporters: reporters}
}

// Report implements Report() method of Reporter by delegating to each underlying reporter.
func (r *compositeReporter) Report(span *Span) {
	for _, reporter := range r.reporters {
		reporter.Report(span)
	}
}

// Close implements Close() method of Reporter by closing each underlying reporter.
func (r *compositeReporter) Close() {
	for _, reporter := range r.reporters {
		reporter.Close()
	}
}

// ------------- REMOTE REPORTER -----------------

type reporterQueueItemType int

const (
	defaultQueueSize           = 100
	defaultBufferFlushInterval = 1 * time.Second
	// defaultShutdownTimeout bounds how long Close() waits for the queue to drain.
	defaultShutdownTimeout = 5 * time.Second

	reporterQueueItemSpan reporterQueueItemType = iota
	reporterQueueItemClose
)

type reporterQueueItem struct {
	itemType reporterQueueItemType
	span     *Span
}

// reporterStats implements reporterstats.ReporterStats.
type reporterStats struct {
	droppedCount int64 // provided to Transports to report data loss to the backend
}

// SpansDroppedFromQueue implements reporterstats.ReporterStats.
func (r *reporterStats) SpansDroppedFromQueue() int64 {
	return atomic.LoadInt64(&r.droppedCount)
}

func (r *reporterStats) incDroppedCount() {
	atomic.AddInt64(&r.droppedCount, 1)
}

type remoteReporter struct {
	// These fields must be first in the struct because `sync/atomic` expects 64-bit alignment.
	// Cf. https://github.com/uber/jaeger-client-go/issues/155, https://goo.gl/zW7dgq
	queueLength int64 // used to update metrics.Gauge
	closed      int64 // 0 - not closed, 1 - closed

	reporterOptions

	sender        Transport
	queue         chan reporterQueueItem
	reporterStats *reporterStats
	spiller       SpanSpiller
	// done is closed by the worker once it has processed the close event, i.e.
	// when every span enqueued before Close() was appended and the final flush ran.
	done chan struct{}
	// stop lets Close() release a wedged worker when the shutdown timeout elapses.
	stop chan struct{}
}

// NewRemoteReporter creates a new reporter that sends spans out of process by means of Sender.
// Calls to Report(Span) return immediately (side effect: if internal buffer is full the span is dropped).
// Periodically the transport buffer is flushed even if it hasn't reached max packet size.
// Calls to Close() block until all spans reported prior to the call to Close are flushed,
// or until the shutdown timeout elapses; spans still pending at that point are spilled
// to the configured SpanSpiller so they are not lost.
func NewRemoteReporter(sender Transport, opts ...ReporterOption) Reporter {
	options := reporterOptions{}
	for _, option := range opts {
		option(&options)
	}
	if options.bufferFlushInterval <= 0 {
		options.bufferFlushInterval = defaultBufferFlushInterval
	}
	if options.shutdownTimeout == 0 {
		options.shutdownTimeout = defaultShutdownTimeout
	}
	if options.logger == nil {
		options.logger = log.NullLogger
	}
	if options.metrics == nil {
		options.metrics = NewNullMetrics()
	}
	if options.queueSize <= 0 {
		options.queueSize = defaultQueueSize
	}
	if options.spiller == nil {
		options.spiller = noopSpanSpiller{}
	}
	reporter := &remoteReporter{
		reporterOptions: options,
		sender:          sender,
		queue:           make(chan reporterQueueItem, options.queueSize),
		reporterStats:   new(reporterStats),
		spiller:         options.spiller,
		done:            make(chan struct{}),
		stop:            make(chan struct{}),
	}
	if receiver, ok := sender.(reporterstats.Receiver); ok {
		receiver.SetReporterStats(reporter.reporterStats)
	}
	go reporter.processQueue()
	return reporter
}

// Report implements Report() method of Reporter.
// It passes the span to a background go-routine for submission to Jaeger backend.
// If the internal queue is full, the span is dropped and metrics.ReporterDropped counter is incremented.
// If Report() is called after the reporter has been Close()-ed, the additional spans will not be
// sent to the backend, but the metrics.ReporterDropped counter may not reflect them correctly,
// because some of them may still be successfully added to the queue.
func (r *remoteReporter) Report(span *Span) {
	select {
	// Need to retain the span otherwise it will be released
	case r.queue <- reporterQueueItem{itemType: reporterQueueItemSpan, span: span.Retain()}:
		atomic.AddInt64(&r.queueLength, 1)
	default:
		r.metrics.ReporterDropped.Inc(1)
		r.reporterStats.incDroppedCount()
	}
}

// Close implements Close() method of Reporter. It asks the background worker to
// drain the queue and flush buffered spans, then waits for that to finish or for
// the shutdown timeout to elapse. On timeout the spans still held by the
// reporter are spilled to the SpanSpiller instead of being silently lost.
func (r *remoteReporter) Close() {
	r.logger.Debugf("closing reporter")
	if swapped := atomic.CompareAndSwapInt64(&r.closed, 0, 1); !swapped {
		r.logger.Error("Repeated attempt to close the reporter is ignored")
		return
	}

	// Signal the worker to stop and enqueue the close marker without blocking
	// forever on a full queue. If the timeout is negative, wait with no bound to
	// preserve the historical "drain everything" semantics.
	if r.shutdownTimeout < 0 {
		r.enqueueCloseEventBestEffort()
		<-r.done
		_ = r.sender.Close()
		return
	}

	timedOut := !r.waitForShutdown()
	if timedOut {
		// The worker may be parked inside a non-cancellable transport call. Closing
		// r.stop makes it return as soon as it reaches a select again, and lets the
		// close-marker enqueue and drainQueue proceed without waiting on the worker.
		close(r.stop)
		r.enqueueCloseEventBestEffort()
		r.spillPendingSpans(r.drainQueue())
	}
	_ = r.sender.Close()
}

// waitForShutdown enqueues the close marker and blocks until the worker drains
// the queue, or until the shutdown timeout elapses. It returns false on timeout.
func (r *remoteReporter) waitForShutdown() (completed bool) {
	timer := time.NewTimer(r.shutdownTimeout)
	defer timer.Stop()

	queued := make(chan struct{})
	go func() {
		r.enqueueCloseEventBestEffort()
		close(queued)
	}()

	select {
	case <-r.done:
		return true
	case <-queued:
		// Marker is in the queue; keep waiting for the worker to finish draining.
		select {
		case <-r.done:
			return true
		case <-timer.C:
			return false
		}
	case <-timer.C:
		return false
	}
}

// enqueueCloseEventBestEffort delivers the close marker to the worker without
// blocking forever on a full queue: if r.stop is already closed (shutdown timed
// out), the enqueue is abandoned immediately instead of deadlocking.
func (r *remoteReporter) enqueueCloseEventBestEffort() {
	item := reporterQueueItem{itemType: reporterQueueItemClose}
	select {
	case r.queue <- item:
		atomic.AddInt64(&r.queueLength, 1)
	case <-r.stop:
	}
}

// drainQueue removes and releases every span still buffered in the queue.
// Any close marker encountered is ignored.
func (r *remoteReporter) drainQueue() []*Span {
	var spans []*Span
	for {
		select {
		case item := <-r.queue:
			atomic.AddInt64(&r.queueLength, -1)
			if item.itemType == reporterQueueItemSpan {
				spans = append(spans, item.span)
			}
		default:
			return spans
		}
	}
}

// spillPendingSpans is used when the worker cannot process the close event at
// all; it persists the drained spans and accounts for anything not saved.
func (r *remoteReporter) spillPendingSpans(spans []*Span) {
	if len(spans) == 0 {
		return
	}
	spilled, err := r.spiller.Spill(spans)
	if err != nil {
		r.logger.Error(fmt.Sprintf("failed to spill %d Jaeger spans: %s", len(spans)-spilled, err.Error()))
	} else {
		r.logger.Infof("spilled %d unsent Jaeger spans to disk during shutdown", spilled)
	}
	r.metrics.ReporterDropped.Inc(int64(len(spans) - spilled))
	// The spans were Retain()-ed when enqueued; the worker never gets to release
	// them on this path, so balance the reference count here.
	for _, span := range spans {
		span.Release()
	}
}

// processQueue reads spans from the queue, converts them to Thrift, and stores them in an internal buffer.
// When the buffer length reaches batchSize, it is flushed by submitting the accumulated spans to Jaeger.
// Buffer also gets flushed automatically every batchFlushInterval seconds, just in case the tracer stopped
// reporting new spans.
func (r *remoteReporter) processQueue() {
	defer close(r.done)
	// flush causes the Sender to flush its accumulated spans and clear the buffer
	flush := func() {
		if flushed, err := r.sender.Flush(); err != nil {
			r.metrics.ReporterFailure.Inc(int64(flushed))
			r.logger.Error(fmt.Sprintf("failed to flush Jaeger spans to server: %s", err.Error()))
		} else if flushed > 0 {
			r.metrics.ReporterSuccess.Inc(int64(flushed))
		}
	}

	timer := time.NewTicker(r.bufferFlushInterval)
	for {
		select {
		case <-r.stop:
			timer.Stop()
			return
		case <-timer.C:
			flush()
		case item := <-r.queue:
			atomic.AddInt64(&r.queueLength, -1)
			switch item.itemType {
			case reporterQueueItemSpan:
				span := item.span
				if flushed, err := r.sender.Append(span); err != nil {
					r.metrics.ReporterFailure.Inc(int64(flushed))
					r.logger.Error(fmt.Sprintf("error reporting Jaeger span %q: %s", span.OperationName(), err.Error()))
				} else if flushed > 0 {
					r.metrics.ReporterSuccess.Inc(int64(flushed))
					// to reduce the number of gauge stats, we only emit queue length on flush
					r.metrics.ReporterQueueLength.Update(atomic.LoadInt64(&r.queueLength))
					r.logger.Debugf("flushed %d spans", flushed)
				}
				span.Release()
			case reporterQueueItemClose:
				timer.Stop()
				flush()
				return
			}
		}
	}
}
