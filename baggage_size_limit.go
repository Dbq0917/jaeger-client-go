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
	"sort"
)

// urlQueryEscapeLen returns the size value would have after url.QueryEscape
// without allocating the escaped string. url.QueryEscape leaves alphanumerics
// and the unreserved marks -_.~ untouched, encodes a space as '+' (one byte)
// and percent-encodes every other byte as a three-byte triplet.
func urlQueryEscapeLen(value string) int {
	extra := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' || c == '_' || c == '.' || c == '~' || c == ' ':
		default:
			extra += 2
		}
	}
	return len(value) + extra
}

// BaggageSizeLimitMode determines how the propagator handles baggage whose
// total wire size exceeds the configured limit.
type BaggageSizeLimitMode int

const (
	// BaggageSizeLimitTruncate keeps as many baggage items as possible while
	// staying within the size limit. Items are emitted in a deterministic
	// (sorted) order; once the remaining budget cannot fit the next item, its
	// value is shortened and the remaining items are dropped. The tracing
	// context header itself is never affected, so downstream services can
	// still correlate the trace.
	BaggageSizeLimitTruncate BaggageSizeLimitMode = iota

	// BaggageSizeLimitReject drops every baggage item when the total wire
	// size exceeds the limit. The tracing context header is still propagated,
	// so downstream services can still correlate the trace.
	BaggageSizeLimitReject
)

// defaultMaxBaggageSize is the default budget, in bytes, for the combined
// size of all baggage headers written during injection. It mirrors the W3C
// baggage header size recommendation.
const defaultMaxBaggageSize = 8192

// baggageSizeLimit enforces a total wire-size budget on the baggage items
// written during injection. It only constrains the business-provided baggage;
// the tracing context header (trace/span IDs and flags) is injected
// separately and is never trimmed or rejected.
type baggageSizeLimit struct {
	// maxSize is the maximum combined size, in bytes, of the baggage headers,
	// including the header prefix, the key and the (possibly encoded) value.
	// A non-positive value disables the limit.
	maxSize int

	// mode selects truncation or rejection when the limit is exceeded.
	mode BaggageSizeLimitMode

	// headerPrefix is the prefix prepended to every baggage key.
	headerPrefix string

	// encodedLen returns the wire size of a baggage value without allocating
	// the encoded string, so the hot path stays allocation-free.
	encodedLen func(string) int
}

func newBaggageSizeLimit(maxSize int, mode BaggageSizeLimitMode, headerPrefix string, encodedLen func(string) int) *baggageSizeLimit {
	if maxSize == 0 {
		maxSize = defaultMaxBaggageSize
	}
	return &baggageSizeLimit{
		maxSize:      maxSize,
		mode:         mode,
		headerPrefix: headerPrefix,
		encodedLen:   encodedLen,
	}
}

// enabled reports whether the limit must be enforced.
func (l *baggageSizeLimit) enabled() bool {
	return l.maxSize > 0
}

// headerSize returns the wire size of a single baggage header.
func (l *baggageSizeLimit) headerSize(key, encodedValue string) int {
	return len(l.headerPrefix) + len(key) + len(encodedValue)
}

// totalSize measures the combined wire size of all baggage headers using the
// allocation-free length function, keeping the budget aligned with the real
// header size.
func (l *baggageSizeLimit) totalSize(baggage map[string]string) int {
	total := 0
	for key, value := range baggage {
		total += len(l.headerPrefix) + len(key) + l.encodedLen(value)
	}
	return total
}

// withinLimit returns true when all baggage headers fit within the budget.
// It is the hot path and performs no allocations when the limit is not
// exceeded.
func (l *baggageSizeLimit) withinLimit(baggage map[string]string) bool {
	if !l.enabled() {
		return true
	}
	total := 0
	for key, value := range baggage {
		total += len(l.headerPrefix) + len(key) + l.encodedLen(value)
		if total > l.maxSize {
			return false
		}
	}
	return true
}

// baggageHeader is an injection candidate after the limit is applied.
type baggageHeader struct {
	key   string
	value string
}

// apply filters the baggage down to headers that fit the budget. It returns
// the headers to inject in a deterministic order (smallest headers first,
// ties broken by key). The truncated result is reported when at least one
// item was shortened or dropped, and rejected when the mode rejects the
// whole baggage payload.
func (l *baggageSizeLimit) apply(
	baggage map[string]string,
	encodeValue func(string) string,
) (headers []baggageHeader, truncated bool, rejected bool) {
	if !l.enabled() {
		for key, value := range baggage {
			headers = append(headers, baggageHeader{key: key, value: value})
		}
		return headers, false, false
	}

	if l.mode == BaggageSizeLimitReject {
		if l.totalSize(baggage) <= l.maxSize {
			for key, value := range baggage {
				headers = append(headers, baggageHeader{key: key, value: value})
			}
			return headers, false, false
		}
		return nil, true, true
	}

	items := make([]baggageHeader, 0, len(baggage))
	for key, value := range baggage {
		items = append(items, baggageHeader{key: key, value: value})
	}
	// Smallest headers first so that compact (and usually critical) items,
	// such as correlation IDs, survive while a single oversized item absorbs
	// the truncation. Ties are broken by key for deterministic output.
	sort.Slice(items, func(i, j int) bool {
		left := len(l.headerPrefix) + len(items[i].key) + l.encodedLen(items[i].value)
		right := len(l.headerPrefix) + len(items[j].key) + l.encodedLen(items[j].value)
		if left != right {
			return left < right
		}
		return items[i].key < items[j].key
	})

	remaining := l.maxSize
	for _, item := range items {
		key, value := item.key, item.value
		headerLen := len(l.headerPrefix) + len(key) + l.encodedLen(value)
		if headerLen <= remaining {
			headers = append(headers, baggageHeader{key: key, value: value})
			remaining -= headerLen
			continue
		}

		// The item does not fit as-is. Truncate its value so the header
		// consumes exactly the remaining budget, then stop: there is no
		// budget left for any further items.
		truncated = true
		valueBudget := remaining - len(l.headerPrefix) - len(key)
		if truncatedValue, ok := truncateValue(value, valueBudget, encodeValue); ok {
			headers = append(headers, baggageHeader{key: key, value: truncatedValue})
		}
		break
	}

	if len(headers) < len(baggage) {
		truncated = true
	}
	return headers, truncated, false
}

// truncateValue shortens a raw baggage value so that its encoded size does
// not exceed budget bytes. Raw bytes are removed one at a time from the end,
// which guarantees the cut never lands in the middle of a percent-encoded
// triplet and keeps the resulting header decodable.
func truncateValue(value string, budget int, encodeValue func(string) string) (string, bool) {
	if budget <= 0 {
		return "", false
	}
	cut := len(value)
	for cut > 0 && len(encodeValue(value[:cut])) > budget {
		cut--
	}
	if cut == 0 {
		return "", false
	}
	return value[:cut], true
}
