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
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics/metricstest"
)

func identityEncode(value string) string { return value }
func identityLen(value string) int       { return len(value) }

func httpEncode(value string) string { return url.QueryEscape(value) }

func TestURLQueryEscapeLen(t *testing.T) {
	values := []string{
		"",
		"plain",
		"with space",
		"a:b/c?d=e&f",
		"unicode-世界-!",
		"~tilde-dot_under.123",
		strings.Repeat("x", 4096),
		"%" + strings.Repeat("q", 10),
	}
	for _, value := range values {
		assert.Equal(t, len(url.QueryEscape(value)), urlQueryEscapeLen(value), "value %q", value)
	}
}

func newTestSizeLimit(t *testing.T, maxSize int, mode BaggageSizeLimitMode) *baggageSizeLimit {
	limit := newBaggageSizeLimit(maxSize, mode, TraceBaggageHeaderPrefix, identityLen)
	require.True(t, limit.enabled())
	return limit
}

func TestBaggageSizeLimitWithinLimit(t *testing.T) {
	limit := newTestSizeLimit(t, 100, BaggageSizeLimitTruncate)
	baggage := map[string]string{"key1": "val1", "key2": "val2"}

	assert.True(t, limit.withinLimit(baggage))

	headers, truncated, rejected := limit.apply(baggage, identityEncode)
	assert.False(t, truncated)
	assert.False(t, rejected)
	assert.Len(t, headers, 2)
}

func TestBaggageSizeLimitDisabled(t *testing.T) {
	limit := newBaggageSizeLimit(-1, BaggageSizeLimitTruncate, TraceBaggageHeaderPrefix, identityLen)
	assert.False(t, limit.enabled())

	baggage := map[string]string{"key": strings.Repeat("x", 10000)}
	assert.True(t, limit.withinLimit(baggage))

	headers, truncated, rejected := limit.apply(baggage, identityEncode)
	assert.False(t, truncated)
	assert.False(t, rejected)
	assert.Len(t, headers, 1)
	assert.Equal(t, baggage["key"], headers[0].value)
}

func TestBaggageSizeLimitDefault(t *testing.T) {
	limit := newBaggageSizeLimit(0, BaggageSizeLimitTruncate, TraceBaggageHeaderPrefix, identityLen)
	assert.Equal(t, defaultMaxBaggageSize, limit.maxSize)
	assert.True(t, limit.enabled())
}

func TestBaggageSizeLimitTruncate(t *testing.T) {
	// Header layout: "uberctx-" (8) + key + value.
	// Budget allows the compact item fully and a prefix of the large one.
	limit := newTestSizeLimit(t, 8+5+5+8+5+2, BaggageSizeLimitTruncate)
	baggage := map[string]string{
		"large": "1234567890",
		"small": "hello",
	}

	assert.False(t, limit.withinLimit(baggage))
	headers, truncated, rejected := limit.apply(baggage, identityEncode)
	assert.True(t, truncated)
	assert.False(t, rejected)
	require.Len(t, headers, 2)

	// Deterministic ordering: compact headers survive first.
	assert.Equal(t, "small", headers[0].key)
	assert.Equal(t, "hello", headers[0].value)
	assert.Equal(t, "large", headers[1].key)
	assert.Equal(t, "12", headers[1].value)

	// The emitted headers never exceed the budget.
	total := 0
	for _, header := range headers {
		total += limit.headerSize(header.key, header.value)
	}
	assert.LessOrEqual(t, total, limit.maxSize)

	// The compact item's key and value are both preserved intact.
	assert.Equal(t, "small", headers[0].key)
}

func TestBaggageSizeLimitTruncateDropsItemsThatDoNotFit(t *testing.T) {
	// Budget only fits the compact item; the oversized one must be dropped.
	limit := newTestSizeLimit(t, 8+5+5+1, BaggageSizeLimitTruncate)
	baggage := map[string]string{
		"small": "hello",
		"large": strings.Repeat("w", 100),
	}

	headers, truncated, rejected := limit.apply(baggage, identityEncode)
	assert.True(t, truncated)
	assert.False(t, rejected)
	require.Len(t, headers, 1)
	assert.Equal(t, "small", headers[0].key)
	assert.Equal(t, "hello", headers[0].value)
}

func TestBaggageSizeLimitTruncateEncodedValue(t *testing.T) {
	// url.QueryEscape expands ':' to '%3A' and ' ' to '+'; the budget is
	// measured against the encoded wire size.
	limit := newBaggageSizeLimit(8+3+5, BaggageSizeLimitTruncate, TraceBaggageHeaderPrefix, urlQueryEscapeLen)
	baggage := map[string]string{"key": "ab:c"}

	headers, truncated, rejected := limit.apply(baggage, httpEncode)
	assert.True(t, truncated)
	assert.False(t, rejected)
	require.Len(t, headers, 1)

	encoded := httpEncode(headers[0].value)
	assert.Len(t, encoded, 5)
	// The cut must not split a percent-encoded triplet.
	assert.False(t, strings.HasSuffix(encoded, "%"))
	decoded, err := url.QueryUnescape(encoded)
	require.NoError(t, err)
	assert.Equal(t, "ab:", decoded)
}

func TestBaggageSizeLimitReject(t *testing.T) {
	limit := newTestSizeLimit(t, 10, BaggageSizeLimitReject)
	baggage := map[string]string{"key": strings.Repeat("x", 100)}

	assert.False(t, limit.withinLimit(baggage))
	headers, truncated, rejected := limit.apply(baggage, identityEncode)
	assert.True(t, truncated)
	assert.True(t, rejected)
	assert.Nil(t, headers)

	// Within the limit, reject mode behaves like the normal path.
	small := newBaggageSizeLimit(100, BaggageSizeLimitReject, TraceBaggageHeaderPrefix, identityLen)
	headers, truncated, rejected = small.apply(map[string]string{"key": "val"}, identityEncode)
	assert.False(t, truncated)
	assert.False(t, rejected)
	assert.Len(t, headers, 1)
}

func TestHTTPHeaderPropagatorTruncatesOversizedBaggage(t *testing.T) {
	metricsFactory, metrics := initMetrics()
	propagator := NewHTTPHeaderPropagator(
		getDefaultHeadersConfig(),
		*metrics,
		PropagatorOptions.BaggageSizeLimit(30, BaggageSizeLimitTruncate),
	)

	spanContext := SpanContext{
		traceID:       TraceID{Low: 1},
		spanID:        SpanID(2),
		samplingState: &samplingState{},
		baggage: map[string]string{
			"small": "ok",
			"huge":  strings.Repeat("z", 200),
		},
	}

	carrier := http.Header{}
	require.NoError(t, propagator.Inject(spanContext, opentracing.HTTPHeadersCarrier(carrier)))

	// The tracing context header is always propagated, even when baggage is cut.
	assert.NotEmpty(t, carrier.Get(TraceContextHeaderName))
	// The small item fits and is preserved with its key intact.
	assert.Equal(t, "ok", carrier.Get(TraceBaggageHeaderPrefix+"small"))
	// The oversized item is shortened rather than emitted wholesale.
	hugeHeader := carrier.Get(TraceBaggageHeaderPrefix + "huge")
	assert.NotEmpty(t, hugeHeader)
	assert.Less(t, len(hugeHeader), 200)

	// Total baggage header wire size stays within the configured budget.
	totalBaggageSize := 0
	for key, values := range carrier {
		if strings.HasPrefix(key, TraceBaggageHeaderPrefix) {
			totalBaggageSize += len(key) + len(values[0])
		}
	}
	assert.LessOrEqual(t, totalBaggageSize, 30)

	metricsFactory.AssertCounterMetrics(t, metricstest.ExpectedMetric{
		Name: "jaeger.tracer.baggage_header_truncations", Value: 1,
	})
}

func TestHTTPHeaderPropagatorRejectsOversizedBaggage(t *testing.T) {
	metricsFactory, metrics := initMetrics()
	propagator := NewHTTPHeaderPropagator(
		getDefaultHeadersConfig(),
		*metrics,
		PropagatorOptions.BaggageSizeLimit(10, BaggageSizeLimitReject),
	)

	spanContext := SpanContext{
		traceID:       TraceID{Low: 1},
		spanID:        SpanID(2),
		samplingState: &samplingState{},
		baggage:       map[string]string{"huge": strings.Repeat("z", 200)},
	}

	carrier := http.Header{}
	require.NoError(t, propagator.Inject(spanContext, opentracing.HTTPHeadersCarrier(carrier)))

	// Baggage is dropped entirely, but the trace context survives.
	assert.NotEmpty(t, carrier.Get(TraceContextHeaderName))
	assert.Empty(t, carrier.Get(TraceBaggageHeaderPrefix+"huge"))

	metricsFactory.AssertCounterMetrics(t, metricstest.ExpectedMetric{
		Name: "jaeger.tracer.baggage_header_rejections", Value: 1,
	})
}

func TestTextMapPropagatorNoOverheadWhenWithinLimit(t *testing.T) {
	metricsFactory, metrics := initMetrics()
	propagator := NewTextMapPropagator(
		getDefaultHeadersConfig(),
		*metrics,
		PropagatorOptions.BaggageSizeLimit(defaultMaxBaggageSize, BaggageSizeLimitTruncate),
	)

	baggage := map[string]string{"key1": "val1", "key2": "val2"}
	spanContext := SpanContext{
		traceID:       TraceID{Low: 1},
		spanID:        SpanID(2),
		samplingState: &samplingState{},
		baggage:       baggage,
	}

	carrier := opentracing.TextMapCarrier{}
	require.NoError(t, propagator.Inject(spanContext, carrier))

	assert.Equal(t, "val1", carrier[TraceBaggageHeaderPrefix+"key1"])
	assert.Equal(t, "val2", carrier[TraceBaggageHeaderPrefix+"key2"])

	// No truncation/rejection metrics on the hot path.
	metricsFactory.AssertCounterMetrics(t,
		metricstest.ExpectedMetric{Name: "jaeger.tracer.baggage_header_truncations", Value: 0},
		metricstest.ExpectedMetric{Name: "jaeger.tracer.baggage_header_rejections", Value: 0},
	)
}

func TestInjectWithoutBaggage(t *testing.T) {
	propagator := NewHTTPHeaderPropagator(
		getDefaultHeadersConfig(),
		*NewNullMetrics(),
		PropagatorOptions.BaggageSizeLimit(10, BaggageSizeLimitTruncate),
	)
	spanContext := SpanContext{
		traceID:       TraceID{Low: 1},
		spanID:        SpanID(2),
		samplingState: &samplingState{},
	}

	carrier := http.Header{}
	require.NoError(t, propagator.Inject(spanContext, opentracing.HTTPHeadersCarrier(carrier)))

	assert.NotEmpty(t, carrier.Get(TraceContextHeaderName))
	assert.Len(t, carrier, 1)
}

// TestOversizedBaggageEndToEndCorrelation drives the full reporting pipeline
// (inject -> extract -> child span) with an oversized business baggage item
// and verifies the downstream service can still correlate the trace.
func TestOversizedBaggageEndToEndCorrelation(t *testing.T) {
	metricsFactory, metrics := initMetrics()
	reporter := NewInMemoryReporter()
	tracer, closer := NewTracer(
		"upstream",
		NewConstSampler(true),
		reporter,
		TracerOptions.Metrics(metrics),
		TracerOptions.Injector(opentracing.HTTPHeaders, NewHTTPHeaderPropagator(
			getDefaultHeadersConfig(),
			*metrics,
			PropagatorOptions.BaggageSizeLimit(256, BaggageSizeLimitTruncate),
		)),
		TracerOptions.Extractor(opentracing.HTTPHeaders, NewHTTPHeaderPropagator(
			getDefaultHeadersConfig(),
			*metrics,
			PropagatorOptions.BaggageSizeLimit(256, BaggageSizeLimitTruncate),
		)),
	)
	defer closer.Close()

	rootSpan := tracer.StartSpan("root").(*Span)
	rootSpan.SetBaggageItem("correlation-id", "corr-12345")
	rootSpan.SetBaggageItem("blob", strings.Repeat("A", 4096))

	carrier := http.Header{}
	require.NoError(t, tracer.Inject(rootSpan.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(carrier)))

	// Every propagated header respects a realistic server-side header budget.
	for key, values := range carrier {
		assert.LessOrEqual(t, len(key)+len(values[0]), 256, "header %s exceeds budget", key)
	}

	downstreamContext, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(carrier))
	require.NoError(t, err)

	// Trace identity survives, so the downstream span joins the same trace.
	downstreamSpan := tracer.StartSpan("downstream", opentracing.ChildOf(downstreamContext)).(*Span)
	assert.Equal(t, rootSpan.context.traceID, downstreamSpan.context.traceID)
	assert.Equal(t, rootSpan.context.spanID, downstreamSpan.context.parentID)

	// The key correlation field is preserved even though the oversized blob was cut.
	assert.Equal(t, "corr-12345", downstreamSpan.BaggageItem("correlation-id"))

	downstreamSpan.Finish()
	rootSpan.Finish()

	metricsFactory.AssertCounterMetrics(t, metricstest.ExpectedMetric{
		Name: "jaeger.tracer.baggage_header_truncations", Value: 1,
	})

	spans := reporter.GetSpans()
	require.Len(t, spans, 2)
	assert.Equal(t, spans[0].(*Span).context.traceID, spans[1].(*Span).context.traceID)
}
