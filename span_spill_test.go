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
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSpanSpillerWritesJSONLines(t *testing.T) {
	dir := t.TempDir()
	spiller := NewFileSpanSpiller(dir)

	tracer, closer := NewTracer("spill-service", NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	const total = 5
	spans := make([]*Span, total)
	for i := range spans {
		spans[i] = tracer.StartSpan("op").SetTag("index", i).(*Span)
		spans[i].Finish()
	}

	written, err := spiller.Spill(spans)
	require.NoError(t, err)
	assert.Equal(t, total, written)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, strings.HasPrefix(entries[0].Name(), "jaeger-spans-"))
	assert.True(t, strings.HasSuffix(entries[0].Name(), ".jsonl"))

	content, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSuffix(content, []byte("\n")), []byte("\n"))
	require.Len(t, lines, total)

	for i, line := range lines {
		var record spilledSpan
		require.NoError(t, json.Unmarshal(line, &record), "line %d must be valid JSON", i)
		assert.Equal(t, "op", record.OperationName)
		assert.NotEmpty(t, record.TraceID)
		assert.NotEmpty(t, record.SpanID)
		assert.Equal(t, float64(i), record.Tags["index"])
	}
}

func TestFileSpanSpillerCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "spill")
	spiller := NewFileSpanSpiller(dir)

	tracer, closer := NewTracer("spill-service", NewConstSampler(true), NewNullReporter())
	defer closer.Close()
	span := tracer.StartSpan("op").(*Span)
	span.Finish()

	written, err := spiller.Spill([]*Span{span})
	require.NoError(t, err)
	assert.Equal(t, 1, written)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestFileSpanSpillerUniqueFilesAcrossSpills(t *testing.T) {
	dir := t.TempDir()
	spiller := NewFileSpanSpiller(dir)

	tracer, closer := NewTracer("spill-service", NewConstSampler(true), NewNullReporter())
	defer closer.Close()

	clock := &fixedClock{mut: &sync.Mutex{}}
	spiller.now = clock.nowTime

	for i := 0; i < 3; i++ {
		clock.advance(time.Nanosecond) // guarantee distinct filenames
		span := tracer.StartSpan("op").(*Span)
		span.Finish()
		_, err := spiller.Spill([]*Span{span})
		require.NoError(t, err)
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 3, "each spill must create a new file, never overwrite")
}

func TestFileSpanSpillerEmptyInput(t *testing.T) {
	dir := t.TempDir()
	spiller := NewFileSpanSpiller(dir)

	written, err := spiller.Spill(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, written)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "no file should be created for an empty spill")
}

func TestFileSpanSpillerMkdirFailure(t *testing.T) {
	spiller := NewFileSpanSpiller(t.TempDir())
	spiller.mkdirAll = func(string, os.FileMode) error {
		return errors.New("permission denied")
	}

	written, err := spiller.Spill([]*Span{{operationName: "op"}})
	assert.Error(t, err)
	assert.Equal(t, 0, written)
}

func TestFileSpanSpillerWriteFailureReportsPartialCount(t *testing.T) {
	dir := t.TempDir()
	spiller := NewFileSpanSpiller(dir)
	spiller.openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return os.OpenFile(name, flag, perm)
	}

	tracer, closer := NewTracer("spill-service", NewConstSampler(true), NewNullReporter())
	defer closer.Close()
	spans := make([]*Span, 3)
	for i := range spans {
		spans[i] = tracer.StartSpan("op").(*Span)
		spans[i].Finish()
	}

	var writes int
	spiller.writeFile = func(f *os.File, b []byte) (int, error) {
		writes++
		if writes == 2 {
			return 0, errors.New("disk full")
		}
		return f.Write(b)
	}

	written, err := spiller.Spill(spans)
	assert.Error(t, err)
	assert.Equal(t, 1, written, "only the first span should be counted as persisted")
}

func TestNoopSpanSpiller(t *testing.T) {
	written, err := noopSpanSpiller{}.Spill([]*Span{{operationName: "op"}})
	require.NoError(t, err)
	assert.Equal(t, 0, written)
}

type fixedClock struct {
	mut *sync.Mutex
	now time.Time
}

func (c *fixedClock) nowTime() time.Time {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.now.IsZero() {
		c.now = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	return c.now
}

func (c *fixedClock) advance(d time.Duration) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.now.IsZero() {
		c.now = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	}
	c.now = c.now.Add(d)
}
