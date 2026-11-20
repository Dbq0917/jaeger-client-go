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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SpanSpiller persists spans that the reporter could not flush to the backend
// before its shutdown timeout elapsed. Implementations must be safe to call at
// most once, during reporter shutdown, from a single goroutine.
type SpanSpiller interface {
	// Spill writes the given spans to durable storage and returns the number of
	// spans successfully persisted. An error indicates that some spans could not
	// be saved and must be considered lost.
	Spill(spans []*Span) (int, error)
}

// spilledSpan is the on-disk JSON representation of an unsent span. Only the
// fields needed to reconstruct a trace are materialized; the file format is one
// JSON object per line (JSON Lines).
type spilledSpan struct {
	TraceID       string                 `json:"trace_id"`
	SpanID        string                 `json:"span_id"`
	ParentID      string                 `json:"parent_id,omitempty"`
	OperationName string                 `json:"operation_name"`
	StartTime     time.Time              `json:"start_time"`
	Duration      time.Duration          `json:"duration"`
	Tags          map[string]interface{} `json:"tags,omitempty"`
}

func newSpilledSpan(span *Span) spilledSpan {
	ctx := span.SpanContext()
	record := spilledSpan{
		TraceID:       ctx.TraceID().String(),
		SpanID:        ctx.SpanID().String(),
		ParentID:      ctx.ParentID().String(),
		OperationName: span.OperationName(),
		StartTime:     span.StartTime(),
		Duration:      span.Duration(),
	}
	if tags := span.Tags(); len(tags) > 0 {
		record.Tags = tags
	}
	return record
}

// FileSpanSpiller writes unsent spans to a directory on the local filesystem as
// JSON Lines files. Each Close() that has spans left over creates a new
// timestamped file so independent shutdowns never overwrite each other.
type FileSpanSpiller struct {
	dir       string
	now       func() time.Time
	marshal   func(v interface{}) ([]byte, error)
	mkdirAll  func(path string, perm os.FileMode) error
	openFile  func(name string, flag int, perm os.FileMode) (*os.File, error)
	writeFile func(f *os.File, b []byte) (int, error)
}

// NewFileSpanSpiller creates a SpanSpiller that stores spilled spans under dir.
// The directory (and any missing parents) is created on first use.
func NewFileSpanSpiller(dir string) *FileSpanSpiller {
	return &FileSpanSpiller{
		dir:       dir,
		now:       time.Now,
		marshal:   json.Marshal,
		mkdirAll:  os.MkdirAll,
		openFile:  os.OpenFile,
		writeFile: func(f *os.File, b []byte) (int, error) { return f.Write(b) },
	}
}

// Spill implements SpanSpiller. It writes one JSON object per span, each
// terminated by a newline, and returns how many spans were fully written.
func (s *FileSpanSpiller) Spill(spans []*Span) (int, error) {
	if len(spans) == 0 {
		return 0, nil
	}
	if err := s.mkdirAll(s.dir, 0o755); err != nil {
		return 0, fmt.Errorf("create spill directory %q: %w", s.dir, err)
	}

	fileName := filepath.Join(s.dir, fmt.Sprintf("jaeger-spans-%s.jsonl", s.now().UTC().Format("20060102T150405.000000000")))
	file, err := s.openFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open spill file %q: %w", fileName, err)
	}

	written := 0
	var writeErr error
	for _, span := range spans {
		line, err := s.marshal(newSpilledSpan(span))
		if err != nil {
			writeErr = fmt.Errorf("encode span %q: %w", span.OperationName(), err)
			break
		}
		line = append(line, '\n')
		if _, err := s.writeFile(file, line); err != nil {
			writeErr = fmt.Errorf("write spill file %q: %w", fileName, err)
			break
		}
		written++
	}

	if err := file.Close(); err != nil && writeErr == nil {
		writeErr = fmt.Errorf("close spill file %q: %w", fileName, err)
	}
	if writeErr != nil {
		return written, writeErr
	}
	return written, nil
}

// noopSpanSpiller discards the spans it receives. It is used when no spill
// handler is configured, and makes the shutdown fallback path uniform.
type noopSpanSpiller struct{}

func (noopSpanSpiller) Spill(spans []*Span) (int, error) {
	return 0, nil
}
