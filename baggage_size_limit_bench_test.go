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
	"testing"

	"github.com/opentracing/opentracing-go"
)

func benchmarkInjectBaggage(b *testing.B, propagator *TextMapPropagator) {
	spanContext := SpanContext{
		traceID:       TraceID{Low: 1},
		spanID:        SpanID(2),
		samplingState: &samplingState{},
		baggage: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		carrier := opentracing.HTTPHeadersCarrier(http.Header{})
		if err := propagator.Inject(spanContext, carrier); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInjectBaggageWithSizeLimit(b *testing.B) {
	propagator := NewHTTPHeaderPropagator(
		getDefaultHeadersConfig(),
		*NewNullMetrics(),
		PropagatorOptions.BaggageSizeLimit(defaultMaxBaggageSize, BaggageSizeLimitTruncate),
	)
	benchmarkInjectBaggage(b, propagator)
}

func BenchmarkInjectBaggageWithoutSizeLimit(b *testing.B) {
	propagator := NewHTTPHeaderPropagator(
		getDefaultHeadersConfig(),
		*NewNullMetrics(),
		PropagatorOptions.BaggageSizeLimit(-1, BaggageSizeLimitTruncate),
	)
	benchmarkInjectBaggage(b, propagator)
}
