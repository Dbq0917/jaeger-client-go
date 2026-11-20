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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
	"github.com/uber/jaeger-client-go/utils"
)

const (
	testOperationName          = "op"
	testFirstTimeOperationName = "firstTimeOp"

	testDefaultSamplingProbability = 0.5
	testMaxID                      = uint64(1) << 62
	testDefaultMaxOperations       = 10
)

var (
	testProbabilisticExpectedTags = []Tag{
		{"sampler.type", "probabilistic"},
		{"sampler.param", 0.5},
	}
	testLowerBoundExpectedTags = []Tag{
		{"sampler.type", "lowerbound"},
		{"sampler.param", 0.5},
	}
)

func TestSamplerTags(t *testing.T) {
	prob, err := NewProbabilisticSampler(0.1)
	require.NoError(t, err)
	rate := NewRateLimitingSampler(0.1)
	remote := &RemotelyControlledSampler{}
	remote.setSampler(NewConstSampler(true))
	tests := []struct {
		sampler  SamplerV2
		typeTag  string
		paramTag interface{}
	}{
		{NewConstSampler(true), "const", true},
		{NewConstSampler(false), "const", false},
		{prob, "probabilistic", 0.1},
		{rate, "ratelimiting", 0.1},
		{remote, "const", true},
	}
	for _, test := range tests {
		decision := test.sampler.OnCreateSpan(makeSpan(0, testOperationName))
		assert.Equal(t, makeSamplerTags(test.typeTag, test.paramTag), decision.Tags)
	}
}

func TestProbabilisticSamplerErrors(t *testing.T) {
	_, err := NewProbabilisticSampler(-0.1)
	assert.Error(t, err)
	_, err = NewProbabilisticSampler(1.1)
	assert.Error(t, err)
}

func TestProbabilisticSampler(t *testing.T) {
	sampler, _ := NewProbabilisticSampler(0.5)
	sampled, tags := sampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
	assert.False(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
	sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID - 20}, testOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)

	t.Run("test_64bit_id", func(t *testing.T) {
		sampled, tags := sampler.IsSampled(TraceID{Low: (testMaxID + 10) | 1<<63}, testOperationName)
		assert.False(t, sampled)
		assert.Equal(t, testProbabilisticExpectedTags, tags)
		sampled, tags = sampler.IsSampled(TraceID{Low: (testMaxID - 20) | 1<<63}, testOperationName)
		assert.True(t, sampled)
		assert.Equal(t, testProbabilisticExpectedTags, tags)
	})

	sampler2, _ := NewProbabilisticSampler(0.5)
	assert.True(t, sampler.Equal(sampler2))
	assert.False(t, sampler.Equal(NewConstSampler(true)))
}

func TestProbabilisticSamplerPerformance(t *testing.T) {
	t.Skip("Skipped performance test")
	sampler, _ := NewProbabilisticSampler(0.01)
	rand := utils.NewRand(8736823764)
	var count uint64
	for i := 0; i < 100000000; i++ {
		id := TraceID{Low: uint64(rand.Int63())}
		if sampled, _ := sampler.IsSampled(id, testOperationName); sampled {
			count++
		}
	}
	// println("Sampled:", count, "rate=", float64(count)/float64(100000000))
	// Sampled: 999829 rate= 0.009998290
}

func TestRateLimitingSampler(t *testing.T) {
	sampler := NewRateLimitingSampler(2)
	sampler2 := NewRateLimitingSampler(2)
	sampler3 := NewRateLimitingSampler(3)
	assert.True(t, sampler.Equal(sampler2))
	assert.False(t, sampler.Equal(sampler3))
	assert.False(t, sampler.Equal(NewConstSampler(false)))

	sampler = NewRateLimitingSampler(2)
	sampled, _ := sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.False(t, sampled)

	sampler = NewRateLimitingSampler(0.1)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.False(t, sampled)
}

func TestGuaranteedThroughputProbabilisticSamplerUpdate(t *testing.T) {
	samplingRate := 0.5
	lowerBound := 2.0
	sampler, err := NewGuaranteedThroughputProbabilisticSampler(lowerBound, samplingRate)
	assert.NoError(t, err)

	assert.Equal(t, lowerBound, sampler.lowerBound)
	assert.Equal(t, samplingRate, sampler.samplingRate)

	newSamplingRate := 0.6
	newLowerBound := 1.0
	sampler.update(newLowerBound, newSamplingRate)
	assert.Equal(t, newLowerBound, sampler.lowerBound)
	assert.Equal(t, newSamplingRate, sampler.samplingRate)

	newSamplingRate = 1.1
	sampler.update(newLowerBound, newSamplingRate)
	assert.Equal(t, 1.0, sampler.samplingRate)
}

func TestGuaranteedThroughputProbabilisticSamplerAdaptiveWindow(t *testing.T) {
	const lowerBound = 10.0
	sampler := newGuaranteedThroughputProbabilisticSampler(lowerBound, 1.0)
	defer sampler.Close()

	// control the clock and the window to make the test deterministic
	now := time.Now()
	sampler.throughput.now = func() time.Time { return now }
	sampler.throughput.windowLengthNanos = int64(time.Second)
	sampler.throughput.clockCheckInterval = 1
	sampler.throughput.windowStartNanos = now.UnixNano()

	rand := utils.NewRand(8736823764)
	countSampled := func(n int) int {
		sampled := 0
		for i := 0; i < n; i++ {
			if ok, _ := sampler.IsSampled(TraceID{Low: uint64(rand.Int63())}, testOperationName); ok {
				sampled++
			}
		}
		return sampled
	}

	// cold window: everything is sampled at the configured rate of 1.0
	assert.Equal(t, 5, countSampled(5))

	// the traffic surges; this window is still sampled at the configured rate
	// because the statistics only cover the previous, cold window
	now = now.Add(time.Second)
	assert.Equal(t, 2000, countSampled(2000))

	// the window slides and the statistics now reflect the hot traffic, so
	// the adaptive boundary only lets ~lowerBound traces per second through
	now = now.Add(time.Second)
	countSampled(1) // first call of the window triggers the rotation
	expectedBoundary := uint64(float64(maxRandomNumber) * (lowerBound / 2000))
	assert.Equal(t, expectedBoundary, sampler.throughput.samplingBoundary())
	sampled := countSampled(1999)
	assert.Less(t, sampled, 100, "hot operation must be sampled at a reduced rate, got %d/1999", sampled)

	// the traffic cools off; once the statistics catch up, the constraint is
	// lifted and the operation is sampled at the configured rate again
	now = now.Add(time.Second)
	countSampled(1) // quiet window, keeps the hot statistics for one more window
	now = now.Add(time.Second)
	assert.Equal(t, 3, countSampled(3))
	assert.GreaterOrEqual(t, sampler.throughput.samplingBoundary(), uint64(maxRandomNumber),
		"constraint must be lifted once the traffic cools off")
}

func TestPerOperationSamplerAdaptiveFollowsTrafficDistribution(t *testing.T) {
	const lowerBound = 10.0
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       1.0,
		DefaultLowerBoundTracesPerSecond: lowerBound,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             "op-a",
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 1.0},
			},
			{
				Operation:             "op-b",
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 1.0},
			},
		},
	}
	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	defer sampler.Close()

	now := time.Now()
	for _, op := range []string{"op-a", "op-b"} {
		throughput := sampler.samplers[op].throughput
		throughput.now = func() time.Time { return now }
		throughput.windowLengthNanos = int64(time.Second)
		throughput.clockCheckInterval = 1
		throughput.windowStartNanos = now.UnixNano()
	}

	rand := utils.NewRand(8736823764)
	call := func(operation string, n int) (sampled int) {
		for i := 0; i < n; i++ {
			if sampler.OnCreateSpan(makeSpan(uint64(rand.Int63()), operation)).Sample {
				sampled++
			}
		}
		return sampled
	}

	// window 1: op-a is hot, op-b is cold
	call("op-a", 1000)
	assert.Equal(t, 5, call("op-b", 5))

	// window 2: the distribution flips, op-a becomes cold and op-b becomes hot;
	// the sampling ratios still reflect the previous window
	now = now.Add(time.Second)
	call("op-a", 5)
	call("op-b", 1000)

	// window 3: the statistics caught up with the new distribution, so the
	// previously hot op-a is sampled at the configured rate again while the
	// now hot op-b is throttled towards the lower bound
	now = now.Add(time.Second)
	assert.Equal(t, 5, call("op-a", 5), "cooled down operation must be sampled at the configured rate")
	sampledB := call("op-b", 1000)
	assert.Less(t, sampledB, 100, "heated up operation must be throttled, got %d/1000", sampledB)
	assert.Greater(t, sampledB, 0, "lower bound must still guarantee some samples")
}

func TestAdaptiveThroughputClockCheckInterval(t *testing.T) {
	throughput := newAdaptiveThroughput(time.Second, 1.0)
	require.Equal(t, uint64(defaultAdaptiveClockCheckInterval), throughput.clockCheckInterval)

	now := time.Now()
	throughput.now = func() time.Time { return now }
	throughput.windowStartNanos = now.UnixNano()
	start := now.UnixNano()

	for i := 0; i < 10; i++ {
		throughput.observe()
	}
	now = now.Add(10 * time.Second)
	throughput.observe()
	assert.Equal(t, start, atomic.LoadInt64(&throughput.windowStartNanos),
		"window must not be rotated before the clock check interval is reached")

	for i := 0; i < defaultAdaptiveClockCheckInterval-11; i++ {
		throughput.observe()
	}
	assert.Equal(t, now.UnixNano(), atomic.LoadInt64(&throughput.windowStartNanos),
		"window must be rotated once the clock check interval is reached")
	assert.Less(t, throughput.samplingBoundary(), uint64(maxRandomNumber),
		"call rate above the target must constrain the sampling boundary")
}

func TestAdaptiveSampler(t *testing.T) {
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: testDefaultSamplingProbability},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 1.0,
		PerOperationStrategies:           samplingRates,
	}

	sampler, err := NewAdaptiveSampler(strategies, 42)
	require.NoError(t, err, "deprecated constructor successful")
	assert.Equal(t, 42, sampler.maxOperations)
	sampler.Close()

	sampler = NewPerOperationSampler(PerOperationSamplerParams{Strategies: strategies})
	assert.Equal(t, sampler.maxOperations, 2000, "default MaxOperations applied")
	sampler.Close()

	sampler = NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	defer sampler.Close()

	decision := sampler.OnCreateSpan(makeSpan(testMaxID+10, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testLowerBoundExpectedTags, decision.Tags)

	decision = sampler.OnCreateSpan(makeSpan(testMaxID-20, testOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	decision = sampler.OnCreateSpan(makeSpan(testMaxID+10, testOperationName))
	assert.False(t, decision.Sample)

	// This operation is seen for the first time by the sampler
	decision = sampler.OnCreateSpan(makeSpan(testMaxID, testFirstTimeOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	decision = sampler.OnSetOperationName(makeSpan(testMaxID, testFirstTimeOperationName), testFirstTimeOperationName)
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)

	decision = sampler.OnSetTag(makeSpan(testMaxID, testFirstTimeOperationName), "key", "value")
	assert.False(t, decision.Sample)

	decision = sampler.OnFinishSpan(makeSpan(testMaxID, testFirstTimeOperationName))
	assert.False(t, decision.Sample)
}

func TestPerOperationSampler_String(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             testOperationName,
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 1},
			},
		},
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})

	assert.Equal(t,
		"PerOperationSampler(defaultSampler=ProbabilisticSampler(samplingRate=0.5), lowerBound=2.000000, "+
			"maxOperations=10, operationNameLateBinding=false, numOperations=1,\nsamplers=[\n"+
			"(operationName=op, "+
			"sampler=GuaranteedThroughputProbabilisticSampler(lowerBound=2.000000, samplingRate=1.000000))])",
		sampler.String())
}

func TestAdaptiveSamplerErrors(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             testOperationName,
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: -0.1},
			},
		},
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	assert.Equal(t, 0.0, sampler.samplers[testOperationName].samplingRate)

	strategies.PerOperationStrategies[0].ProbabilisticSampling.SamplingRate = 1.1
	sampler = NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})
	assert.Equal(t, 1.0, sampler.samplers[testOperationName].samplingRate)
}

func TestAdaptiveSamplerUpdate(t *testing.T) {
	samplingRate := 0.1
	lowerBound := 2.0
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: samplingRate},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: lowerBound,
		PerOperationStrategies:           samplingRates,
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: testDefaultMaxOperations,
		Strategies:    strategies,
	})

	assert.Equal(t, lowerBound, sampler.lowerBound)
	assert.Equal(t, testDefaultSamplingProbability, sampler.defaultSampler.SamplingRate())
	assert.Len(t, sampler.samplers, 1)

	// Update the sampler with new sampling rates
	newSamplingRate := 0.2
	newLowerBound := 3.0
	newDefaultSamplingProbability := 0.1
	newSamplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: newSamplingRate},
		},
		{
			Operation:             testFirstTimeOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: newSamplingRate},
		},
	}
	strategies = &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       newDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: newLowerBound,
		PerOperationStrategies:           newSamplingRates,
	}

	sampler.update(strategies)
	assert.Equal(t, newLowerBound, sampler.lowerBound)
	assert.Equal(t, newDefaultSamplingProbability, sampler.defaultSampler.SamplingRate())
	assert.Len(t, sampler.samplers, 2)
}

func TestMaxOperations(t *testing.T) {
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 0.1},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies:           samplingRates,
	}

	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: 1,
		Strategies:    strategies,
	})

	decision := sampler.OnCreateSpan(makeSpan(testMaxID-10, testFirstTimeOperationName))
	assert.True(t, decision.Sample)
	assert.Equal(t, testProbabilisticExpectedTags, decision.Tags)
}

func TestAdaptiveSamplerDoesNotApplyToChildrenSpans(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       0,
		DefaultLowerBoundTracesPerSecond: 0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             "op1",
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 0.0},
			},
			{
				Operation:             "op2",
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 1.0},
			},
		},
	}
	sampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations:            1,
		OperationNameLateBinding: true, // these tests rely on late binding
		Strategies:               strategies,
	})
	tracer, closer := NewTracer("service", sampler, NewNullReporter())
	defer closer.Close()

	_ = tracer.StartSpan("op1") // exhaust lower bound sampler once
	span1 := tracer.StartSpan("op1")
	assert.False(t, span1.Context().(SpanContext).IsSampled(), "op1 should not be sampled on root span")
	span1.SetOperationName("op2")
	assert.True(t, span1.Context().(SpanContext).IsSampled(), "op2 should be sampled on root span")

	span2 := tracer.StartSpan("op2")
	assert.True(t, span2.Context().(SpanContext).IsSampled(), "op2 should be sampled on root span")

	parent := tracer.StartSpan("op1")
	assert.False(t, parent.Context().(SpanContext).IsSampled(), "parent span should not be sampled")
	assert.False(t, parent.Context().(SpanContext).IsSamplingFinalized(), "parent span should not be finalized")

	child := tracer.StartSpan("op2", opentracing.ChildOf(parent.Context()))
	assert.False(t, child.Context().(SpanContext).IsSampled(), "child span should not be sampled even with op2")
	assert.False(t, child.Context().(SpanContext).IsSamplingFinalized(), "child span should not be finalized")
	child.SetOperationName("op2")
	assert.False(t, child.Context().(SpanContext).IsSampled(), "op2 should not be sampled on the child span")
	assert.True(t, child.Context().(SpanContext).IsSamplingFinalized(), "child span should be finalized after setOperationName")
}

func TestAdaptiveSampler_lockRaceCondition(t *testing.T) {
	agent, remoteSampler, _ := initAgent(t)
	defer agent.Close()
	remoteSampler.Close() // stop timer-based updates, we want to call them manually

	numOperations := 1000
	adaptiveSampler := NewPerOperationSampler(PerOperationSamplerParams{
		MaxOperations: 2000,
		Strategies: &sampling.PerOperationSamplingStrategies{
			DefaultSamplingProbability: 1,
		},
	})
	// Overwrite the sampler with an adaptive sampler
	remoteSampler.setSampler(adaptiveSampler)

	tracer, closer := NewTracer("service", remoteSampler, NewNullReporter())
	defer closer.Close()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(2)

	isSampled := func(t *testing.T, remoteSampler *RemotelyControlledSampler, numOperations int, operationNamePrefix string) {
		for i := 0; i < numOperations; i++ {
			runtime.Gosched()
			span := tracer.StartSpan(fmt.Sprintf("%s%d", operationNamePrefix, i))
			assert.True(t, span.Context().(SpanContext).IsSampled())
		}
	}

	// Start 2 go routines that will simulate simultaneous calls to IsSampled
	go func() {
		defer wg.Done()
		isSampled(t, remoteSampler, numOperations, "a")
	}()
	go func() {
		defer wg.Done()
		isSampled(t, remoteSampler, numOperations, "b")
	}()
}
