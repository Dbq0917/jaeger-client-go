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

	reporterQueueItemSpan reporterQueueItemType = iota
	reporterQueueItemClose
)

type reporterQueueItem struct {
	itemType reporterQueueItemType
	span     *Span
	close    *sync.WaitGroup
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
	retries       *retryQueue
	retryTimer    *time.Timer
}

// NewRemoteReporter creates a new reporter that sends spans out of process by means of Sender.
// Calls to Report(Span) return immediately (side effect: if internal buffer is full the span is dropped).
// Periodically the transport buffer is flushed even if it hasn't reached max packet size.
// Calls to Close() block until all spans reported prior to the call to Close are flushed.
func NewRemoteReporter(sender Transport, opts ...ReporterOption) Reporter {
	options := reporterOptions{}
	for _, option := range opts {
		option(&options)
	}
	if options.bufferFlushInterval <= 0 {
		options.bufferFlushInterval = defaultBufferFlushInterval
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
	if options.retryQueueSize <= 0 {
		options.retryQueueSize = defaultRetryQueueSize
	}
	if options.retryInitialInterval <= 0 {
		options.retryInitialInterval = defaultRetryInitialInterval
	}
	if options.retryMaxInterval <= 0 {
		options.retryMaxInterval = defaultRetryMaxInterval
	}
	if options.retryReplayInterval <= 0 {
		options.retryReplayInterval = defaultRetryReplayInterval
	}
	reporter := &remoteReporter{
		reporterOptions: options,
		sender:          sender,
		queue:           make(chan reporterQueueItem, options.queueSize),
		reporterStats:   new(reporterStats),
		retries:         newRetryQueue(options),
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

// Close implements Close() method of Reporter by waiting for the queue to be drained.
func (r *remoteReporter) Close() {
	r.logger.Debugf("closing reporter")
	if swapped := atomic.CompareAndSwapInt64(&r.closed, 0, 1); !swapped {
		r.logger.Error("Repeated attempt to close the reporter is ignored")
		return
	}
	r.sendCloseEvent()
	_ = r.sender.Close()
}

func (r *remoteReporter) sendCloseEvent() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	item := reporterQueueItem{itemType: reporterQueueItemClose, close: wg}

	r.queue <- item // if the queue is full we will block until there is space
	atomic.AddInt64(&r.queueLength, 1)
	wg.Wait()
}

// processQueue reads spans from the queue, converts them to Thrift, and stores them in an internal buffer.
// When the buffer length reaches batchSize, it is flushed by submitting the accumulated spans to Jaeger.
// Buffer also gets flushed automatically every batchFlushInterval seconds, just in case the tracer stopped
// reporting new spans.
//
// Reference ownership: each span dequeued from the main queue carries one
// reference that is always released at the end of processing. When a flush
// fails the affected spans are additionally retained and handed to the bounded
// retry queue, which owns that extra reference and releases it after a
// successful replay, on capacity drop, or on shutdown.
func (r *remoteReporter) processQueue() {
	// pending mirrors the spans currently buffered inside the sender, in
	// append order, so a failed flush knows which spans to enqueue for retry.
	pending := make([]*Span, 0, 64)

	// flush forces the sender to send its buffered spans.
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = make([]*Span, 0, cap(pending))
		flushed, err := r.sender.Flush()
		if err != nil {
			r.metrics.ReporterFailure.Inc(int64(flushed))
			r.enqueueRetry(batch)
			r.logger.Error(fmt.Sprintf("failed to flush Jaeger spans to server: %s", err.Error()))
			return
		}
		if flushed > 0 {
			r.metrics.ReporterSuccess.Inc(int64(flushed))
		}
	}

	timer := time.NewTicker(r.bufferFlushInterval)
	// retryTimer is lazily armed only when the retry queue is non-empty, so a
	// healthy reporter never performs retry scheduling work.
	retryTimer := time.NewTimer(time.Hour)
	retryTimer.Stop()
	r.retryTimer = retryTimer
	for {
		select {
		case <-timer.C:
			flush()
		case <-retryTimer.C:
			r.processRetries(retryTimer)
		case item := <-r.queue:
			atomic.AddInt64(&r.queueLength, -1)
			switch item.itemType {
			case reporterQueueItemSpan:
				span := item.span
				flushed, err := r.sender.Append(span)
				// flushed counts spans handed to the backend during this append,
				// which includes the previously buffered spans mirrored in
				// pending and, for well-behaved senders, the current span. The
				// pending slice must only ever shed its own entries.
				pendingFlushed := flushed
				if pendingFlushed > len(pending) {
					pendingFlushed = len(pending)
				}
				if err != nil {
					r.metrics.ReporterFailure.Inc(int64(flushed))
					r.logger.Error(fmt.Sprintf("error reporting Jaeger span %q: %s", span.OperationName(), err.Error()))
					if pendingFlushed > 0 {
						// The sender flushed its previously buffered spans (the
						// first `pendingFlushed` pending entries) and failed; the
						// current span stays buffered in the sender.
						r.enqueueRetry(append([]*Span(nil), pending[:pendingFlushed]...))
						pending = append([]*Span(nil), pending[pendingFlushed:]...)
					}
				} else if flushed > 0 {
					r.metrics.ReporterSuccess.Inc(int64(flushed))
					// to reduce the number of gauge stats, we only emit queue length on flush
					r.metrics.ReporterQueueLength.Update(atomic.LoadInt64(&r.queueLength))
					r.logger.Debugf("flushed %d spans", flushed)
					pending = append([]*Span(nil), pending[pendingFlushed:]...)
				}
				pending = append(pending, span)
				span.Release()
			case reporterQueueItemClose:
				timer.Stop()
				retryTimer.Stop()
				r.drainRetriesOnClose()
				flush()
				item.close.Done()
				return
			}
		}
	}
}

// enqueueRetry moves a failed batch into the bounded retry queue. The spans
// gain an additional reference owned by the queue; their original reference
// from the main queue is released separately by the processing loop. When the
// queue is full the oldest spans are dropped and accounted for in the metrics.
func (r *remoteReporter) enqueueRetry(spans []*Span) {
	if len(spans) == 0 {
		return
	}
	for _, span := range spans {
		span.Retain()
	}
	now := r.now()
	wasEmpty := r.retries.len() == 0
	dropped := r.retries.enqueue(spans)
	if dropped > 0 {
		r.metrics.ReporterRetryDropped.Inc(int64(dropped))
		r.metrics.ReporterDropped.Inc(int64(dropped))
	}
	r.metrics.ReporterRetryQueueLength.Update(int64(r.retries.len()))
	if wasEmpty {
		r.retries.armBackoff(now)
		// Arm the (otherwise idle) retry timer so processRetries actually runs.
		r.retryTimer.Reset(r.retryWait(now))
	}
}

// processRetries replays at most one batch when the retry timer fires, then
// re-arms the timer according to the current backoff / pacing state.
func (r *remoteReporter) processRetries(retryTimer *time.Timer) {
	if r.retries.len() == 0 {
		return
	}
	now := r.now()
	if !r.retries.ready(now) {
		retryTimer.Reset(r.retryWait(now))
		return
	}
	batch := r.retries.pop()
	if r.resend(batch.spans) {
		r.retries.onSuccess(now)
		r.metrics.ReporterRetrySuccess.Inc(int64(len(batch.spans)))
	} else {
		r.retries.onFailure(now)
		r.metrics.ReporterRetryFailure.Inc(int64(len(batch.spans)))
	}
	r.metrics.ReporterRetryQueueLength.Update(int64(r.retries.len()))
	if r.retries.len() > 0 {
		retryTimer.Reset(r.retryWait(r.now()))
	} else {
		r.retries.resetPacing()
	}
}

// retryWait returns the delay until the next permitted replay attempt: the
// remaining backoff while the backend is failing, otherwise the fixed replay
// interval that paces draining of a recovered backlog.
func (r *remoteReporter) retryWait(now time.Time) time.Duration {
	if d := r.retries.nextAttempt.Sub(now); d > 0 {
		return d
	}
	return r.retryReplayInterval
}

// resend feeds a queued batch back through the sender and reports whether it
// was fully accepted and flushed. The retry queue retains span ownership in
// either case (it releases the spans only on success via onSuccess).
func (r *remoteReporter) resend(spans []*Span) (success bool) {
	for _, span := range spans {
		if flushed, err := r.sender.Append(span); err != nil {
			if flushed > 0 {
				r.metrics.ReporterFailure.Inc(int64(flushed))
			}
			return false
		} else if flushed > 0 {
			r.metrics.ReporterSuccess.Inc(int64(flushed))
		}
	}
	if flushed, err := r.sender.Flush(); err != nil {
		r.metrics.ReporterFailure.Inc(int64(flushed))
		r.logger.Error(fmt.Sprintf("failed to replay Jaeger spans to server: %s", err.Error()))
		return false
	} else if flushed > 0 {
		r.metrics.ReporterSuccess.Inc(int64(flushed))
	}
	return true
}

// drainRetriesOnClose makes a best-effort, non-backoff attempt to send queued
// spans on shutdown. Spans that still cannot be sent are released to avoid
// leaking pooled span objects.
func (r *remoteReporter) drainRetriesOnClose() {
	for r.retries.len() > 0 {
		batch := r.retries.pop()
		if r.resend(batch.spans) {
			r.retries.onSuccess(time.Now())
			r.metrics.ReporterRetrySuccess.Inc(int64(len(batch.spans)))
			continue
		}
		r.metrics.ReporterRetryFailure.Inc(int64(len(batch.spans)))
		r.retries.releaseAll()
		break
	}
	r.metrics.ReporterRetryQueueLength.Update(0)
}

// now is an indirection so tests can control time deterministically.
func (r *remoteReporter) now() time.Time {
	return time.Now()
}
