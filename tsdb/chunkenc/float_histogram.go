// Copyright 2021 The Prometheus Authors
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

package chunkenc

import (
	"encoding/binary"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
)

// FloatHistogramChunk holds encoded sample data for a sparse, high-resolution
// histogram.
//
// Each sample has multiple "fields", stored in the following way (raw = store
// number directly, delta = store delta to the previous number, dod = store
// delta of the delta to the previous number, xor = what we do for regular
// sample values):
//
//	field →    ts    count zeroCount sum []posbuckets []negbuckets
//	sample 1   raw   raw   raw       raw []raw        []raw
//	sample 2   delta xor   xor       xor []xor        []xor
//	sample >2  dod   xor   xor       xor []xor        []xor
type FloatHistogramChunk struct {
	b bstream
}

// NewFloatHistogramChunk returns a new chunk with float histogram encoding of the given
// size.
func NewFloatHistogramChunk() *FloatHistogramChunk {
	b := make([]byte, 3, 128)
	return &FloatHistogramChunk{b: bstream{stream: b, count: 0}}
}

// Encoding returns the encoding type.
func (c *FloatHistogramChunk) Encoding() Encoding {
	return EncFloatHistogram
}

// Bytes returns the underlying byte slice of the chunk.
func (c *FloatHistogramChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *FloatHistogramChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

// Layout returns the float histogram layout. Only call this on chunks that have at
// least one sample.
func (c *FloatHistogramChunk) Layout() (
	schema int32, zeroThreshold float64,
	negativeSpans, positiveSpans []histogram.Span,
	err error,
) {
	if c.NumSamples() == 0 {
		panic("FloatHistogramChunk.Layout() called on an empty chunk")
	}
	b := newBReader(c.Bytes()[2:])
	return readHistogramChunkLayout(&b)
}

// SetCounterResetHeader sets the counter reset header.
func (c *FloatHistogramChunk) SetCounterResetHeader(h CounterResetHeader) {
	switch h {
	case CounterReset, NotCounterReset, GaugeType, UnknownCounterReset:
		bytes := c.Bytes()
		bytes[2] = (bytes[2] & 0b00111111) | byte(h)
	default:
		panic("invalid CounterResetHeader type")
	}
}

// GetCounterResetHeader returns the info about the first 2 bits of the chunk
// header.
func (c *FloatHistogramChunk) GetCounterResetHeader() CounterResetHeader {
	return CounterResetHeader(c.Bytes()[2] & 0b11000000)
}

// Compact implements the Chunk interface.
func (c *FloatHistogramChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

// Appender implements the Chunk interface.
func (c *FloatHistogramChunk) Appender() (Appender, error) {
	it := c.iterator(nil)

	// To get an appender, we must know the state it would have if we had
	// appended all existing data from scratch. We iterate through the end
	// and populate via the iterator's state.
	for it.Next() == ValFloatHistogram {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	a := &FloatHistogramAppender{
		b: &c.b,

		schema:        it.schema,
		zThreshold:    it.zThreshold,
		pSpans:        it.pSpans,
		nSpans:        it.nSpans,
		t:             it.t,
		cnt:           it.cnt,
		zCnt:          it.zCnt,
		tDelta:        it.tDelta,
		cntDelta:      it.cntDelta,
		zCntDelta:     it.zCntDelta,
		pBuckets:      it.pBuckets,
		nBuckets:      it.nBuckets,
		pBucketsDelta: it.pBucketsDelta,
		nBucketsDelta: it.nBucketsDelta,

		sum:      it.sum,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if it.numTotal == 0 {
		a.leading = 0xff
	}
	return a, nil
}

func newFloatHistogramIterator(b []byte) *floatHistogramIterator {
	it := &floatHistogramIterator{
		br:       newBReader(b),
		numTotal: binary.BigEndian.Uint16(b),
		t:        math.MinInt64,
	}
	// The first 3 bytes contain chunk headers.
	// We skip that for actual samples.
	_, _ = it.br.readBits(24)
	return it
}

func (c *FloatHistogramChunk) iterator(it Iterator) *floatHistogramIterator {
	// This comment is copied from XORChunk.iterator:
	//   Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	//   When using striped locks to guard access to chunks, probably yes.
	//   Could only copy data if the chunk is not completed yet.
	if floatHistogramIter, ok := it.(*floatHistogramIterator); ok {
		floatHistogramIter.Reset(c.b.bytes())
		return floatHistogramIter
	}
	return newFloatHistogramIterator(c.b.bytes())
}

// Iterator implements the Chunk interface.
func (c *FloatHistogramChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

type xorValue struct {
	value    float64
	leading  uint8
	trailing uint8
}

// FloatHistogramAppender is an Appender implementation for float histograms.
type FloatHistogramAppender struct {
	b *bstream

	// Layout:
	schema         int32
	zThreshold     float64
	pSpans, nSpans []histogram.Span

	t                  int64
	tDelta             int64
	sum, cnt, zCnt     xorValue
	pBuckets, nBuckets []xorValue
}

// Append implements Appender. This implementation panics because normal float
// samples must never be appended to a float histogram chunk.
func (a *FloatHistogramAppender) Append(int64, float64) {
	panic("appended a float sample to a float histogram chunk")
}

// Appendable returns whether the chunk can be appended to, and if so
// whether any recoding needs to happen using the provided interjections
// (in case of any new buckets, positive or negative range, respectively).
//
// The chunk is not appendable in the following cases:
//
// • The schema has changed.
//
// • The threshold for the zero bucket has changed.
//
// • Any buckets have disappeared.
//
// • There was a counter reset in the count of observations or in any bucket,
// including the zero bucket.
//
// • The last sample in the chunk was stale while the current sample is not stale.
//
// The method returns an additional boolean set to true if it is not appendable
// because of a counter reset. If the given sample is stale, it is always ok to
// append. If counterReset is true, okToAppend is always false.
func (a *FloatHistogramAppender) Appendable(h *histogram.FloatHistogram) (
	positiveInterjections, negativeInterjections []Interjection,
	okToAppend, counterReset bool,
) {
	if value.IsStaleNaN(h.Sum) {
		// This is a stale sample whose buckets and spans don't matter.
		okToAppend = true
		return
	}
	if value.IsStaleNaN(a.sum.value) {
		// If the last sample was stale, then we can only accept stale
		// samples in this chunk.
		return
	}

	if h.Count < a.cnt.value {
		// There has been a counter reset.
		counterReset = true
		return
	}

	if h.Schema != a.schema || h.ZeroThreshold != a.zThreshold {
		return
	}

	if h.ZeroCount < a.zCnt.value {
		// There has been a counter reset since ZeroThreshold didn't change.
		counterReset = true
		return
	}

	var ok bool
	positiveInterjections, ok = compareSpans(a.pSpans, h.PositiveSpans)
	if !ok {
		counterReset = true
		return
	}
	negativeInterjections, ok = compareSpans(a.nSpans, h.NegativeSpans)
	if !ok {
		counterReset = true
		return
	}

	if counterResetInAnyFloatBucket(a.pBuckets, h.PositiveBuckets, a.pSpans, h.PositiveSpans) ||
		counterResetInAnyFloatBucket(a.nBuckets, h.NegativeBuckets, a.nSpans, h.NegativeSpans) {
		counterReset, positiveInterjections, negativeInterjections = true, nil, nil
		return
	}

	okToAppend = true
	return
}

// counterResetInAnyBucket returns true if there was a counter reset for any
// bucket. This should be called only when the bucket layout is the same or new
// buckets were added. It does not handle the case of buckets missing.
func counterResetInAnyFloatBucket(oldBuckets []xorValue, newBuckets []float64, oldSpans, newSpans []histogram.Span) bool {
	if len(oldSpans) == 0 || len(oldBuckets) == 0 {
		return false
	}

	oldSpanSliceIdx, newSpanSliceIdx := 0, 0                   // Index for the span slices.
	oldInsideSpanIdx, newInsideSpanIdx := uint32(0), uint32(0) // Index inside a span.
	oldIdx, newIdx := oldSpans[0].Offset, newSpans[0].Offset

	oldBucketSliceIdx, newBucketSliceIdx := 0, 0 // Index inside bucket slice.

	// Since we assume that new spans won't have missing buckets, there will never be a case
	// where the old index will not find a matching new index.
	for {
		if oldIdx == newIdx {
			if newBuckets[newBucketSliceIdx] < oldBuckets[oldBucketSliceIdx].value {
				return true
			}
		}

		if oldIdx <= newIdx {
			// Moving ahead old bucket and span by 1 index.
			if oldInsideSpanIdx == oldSpans[oldSpanSliceIdx].Length-1 {
				// Current span is over.
				oldSpanSliceIdx++
				oldInsideSpanIdx = 0
				if oldSpanSliceIdx >= len(oldSpans) {
					// All old spans are over.
					break
				}
				oldIdx += 1 + oldSpans[oldSpanSliceIdx].Offset
			} else {
				oldInsideSpanIdx++
				oldIdx++
			}
			oldBucketSliceIdx++
		}

		if oldIdx > newIdx {
			// Moving ahead new bucket and span by 1 index.
			if newInsideSpanIdx == newSpans[newSpanSliceIdx].Length-1 {
				// Current span is over.
				newSpanSliceIdx++
				newInsideSpanIdx = 0
				if newSpanSliceIdx >= len(newSpans) {
					// All new spans are over.
					// This should not happen, old spans above should catch this first.
					panic("new spans over before old spans in counterReset")
				}
				newIdx += 1 + newSpans[newSpanSliceIdx].Offset
			} else {
				newInsideSpanIdx++
				newIdx++
			}
			newBucketSliceIdx++
		}
	}

	return false
}

// AppendFloatHistogram appends a float histogram to the chunk. The caller must ensure that
// the float histogram is properly structured, e.g. the number of buckets used
// corresponds to the number conveyed by the span structures. First call
// Appendable() and act accordingly!
func (a *FloatHistogramAppender) AppendFloatHistogram(t int64, fh *histogram.FloatHistogram) {
	var tDelta int64
	num := binary.BigEndian.Uint16(a.b.bytes())

	if value.IsStaleNaN(fh.Sum) {
		// Emptying out other fields to write no buckets, and an empty
		// layout in case of first histogram in the chunk.
		fh = &histogram.FloatHistogram{Sum: fh.Sum}
	}

	if num == 0 {
		// The first append gets the privilege to dictate the layout
		// but it's also responsible for encoding it into the chunk!
		writeHistogramChunkLayout(a.b, fh.Schema, fh.ZeroThreshold, fh.PositiveSpans, fh.NegativeSpans)
		a.schema = fh.Schema
		a.zThreshold = fh.ZeroThreshold

		if len(fh.PositiveSpans) > 0 {
			a.pSpans = make([]histogram.Span, len(fh.PositiveSpans))
			copy(a.pSpans, fh.PositiveSpans)
		} else {
			a.pSpans = nil
		}
		if len(fh.NegativeSpans) > 0 {
			a.nSpans = make([]histogram.Span, len(fh.NegativeSpans))
			copy(a.nSpans, fh.NegativeSpans)
		} else {
			a.nSpans = nil
		}

		numPBuckets, numNBuckets := countSpans(fh.PositiveSpans), countSpans(fh.NegativeSpans)
		if numPBuckets > 0 {
			a.pBuckets = make([]xorValue, numPBuckets)
		} else {
			a.pBuckets = nil
		}
		if numNBuckets > 0 {
			a.nBuckets = make([]xorValue, numNBuckets)
		} else {
			a.nBuckets = nil
		}

		// Now store the actual data.
		putVarbitInt(a.b, t)
		a.b.writeBits(math.Float64bits(fh.Count), 64)
		a.b.writeBits(math.Float64bits(fh.ZeroCount), 64)
		a.b.writeBits(math.Float64bits(fh.Sum), 64)
		a.cnt.value = fh.Count
		a.zCnt.value = fh.ZeroCount
		a.sum.value = fh.Sum
		for i, b := range fh.PositiveBuckets {
			a.b.writeBits(math.Float64bits(b), 64)
			a.pBuckets[i].value = b
		}
		for i, b := range fh.NegativeBuckets {
			a.b.writeBits(math.Float64bits(b), 64)
			a.nBuckets[i].value = b
		}
	} else {
		// The case for the 2nd sample with single deltas is implicitly handled correctly with the double delta code,
		// so we don't need a separate single delta logic for the 2nd sample.

		tDelta = t - a.t

		tDod := tDelta - a.tDelta

		putVarbitInt(a.b, tDod)

		a.writeXorValue(&a.cnt, fh.Count)
		a.writeXorValue(&a.zCnt, fh.ZeroCount)
		a.writeXorValue(&a.sum, fh.Sum)

		for i, b := range fh.PositiveBuckets {
			a.writeXorValue(&a.pBuckets[i], b)
		}
		for i, b := range fh.NegativeBuckets {
			a.writeXorValue(&a.nBuckets[i], b)
		}
	}

	binary.BigEndian.PutUint16(a.b.bytes(), num+1)

	a.t = t
	a.tDelta = tDelta
}

// Recode converts the current chunk to accommodate an expansion of the set of
// (positive and/or negative) buckets used, according to the provided
// interjections, resulting in the honoring of the provided new positive and
// negative spans. To continue appending, use the returned Appender rather than
// the receiver of this method.
func (a *HistogramAppender) Recode(
	positiveInterjections, negativeInterjections []Interjection,
	positiveSpans, negativeSpans []histogram.Span,
) (Chunk, Appender) {
	// TODO(beorn7): This currently just decodes everything and then encodes
	// it again with the new span layout. This can probably be done in-place
	// by editing the chunk. But let's first see how expensive it is in the
	// big picture. Also, in-place editing might create concurrency issues.
	byts := a.b.bytes()
	it := newHistogramIterator(byts)
	hc := NewHistogramChunk()
	app, err := hc.Appender()
	if err != nil {
		panic(err)
	}
	numPositiveBuckets, numNegativeBuckets := countSpans(positiveSpans), countSpans(negativeSpans)

	for it.Next() == ValHistogram {
		tOld, hOld := it.AtHistogram()

		// We have to newly allocate slices for the modified buckets
		// here because they are kept by the appender until the next
		// append.
		// TODO(beorn7): We might be able to optimize this.
		var positiveBuckets, negativeBuckets []int64
		if numPositiveBuckets > 0 {
			positiveBuckets = make([]int64, numPositiveBuckets)
		}
		if numNegativeBuckets > 0 {
			negativeBuckets = make([]int64, numNegativeBuckets)
		}

		// Save the modified histogram to the new chunk.
		hOld.PositiveSpans, hOld.NegativeSpans = positiveSpans, negativeSpans
		if len(positiveInterjections) > 0 {
			hOld.PositiveBuckets = interject(hOld.PositiveBuckets, positiveBuckets, positiveInterjections)
		}
		if len(negativeInterjections) > 0 {
			hOld.NegativeBuckets = interject(hOld.NegativeBuckets, negativeBuckets, negativeInterjections)
		}
		app.AppendHistogram(tOld, hOld)
	}

	hc.SetCounterResetHeader(CounterResetHeader(byts[2] & 0b11000000))
	return hc, app
}

func (a *FloatHistogramAppender) writeXorValue(old *xorValue, v float64) {
	xorWrite(a.b, v, old.value, &old.leading, &old.trailing)
	old.value = v
}

type floatHistogramIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	// Layout:
	schema         int32
	zThreshold     float64
	pSpans, nSpans []histogram.Span

	// For the fields that are tracked as deltas and ultimately dod's.
	t                            int64
	cnt, zCnt                    uint64
	tDelta, cntDelta, zCntDelta  int64
	pBuckets, nBuckets           []int64   // Delta between buckets.
	pFloatBuckets, nFloatBuckets []float64 // Absolute counts.
	pBucketsDelta, nBucketsDelta []int64

	// The sum is Gorilla xor encoded.
	sum      float64
	leading  uint8
	trailing uint8

	// Track calls to retrieve methods. Once they have been called, we
	// cannot recycle the bucket slices anymore because we have returned
	// them in the histogram.
	atHistogramCalled, atFloatHistogramCalled bool

	err error
}

func (it *histogramIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}

	for t > it.t || it.numRead == 0 {
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValHistogram
}

func (it *histogramIterator) At() (int64, float64) {
	panic("cannot call histogramIterator.At")
}

func (it *histogramIterator) AtHistogram() (int64, *histogram.Histogram) {
	if value.IsStaleNaN(it.sum) {
		return it.t, &histogram.Histogram{Sum: it.sum}
	}
	it.atHistogramCalled = true
	return it.t, &histogram.Histogram{
		Count:           it.cnt,
		ZeroCount:       it.zCnt,
		Sum:             it.sum,
		ZeroThreshold:   it.zThreshold,
		Schema:          it.schema,
		PositiveSpans:   it.pSpans,
		NegativeSpans:   it.nSpans,
		PositiveBuckets: it.pBuckets,
		NegativeBuckets: it.nBuckets,
	}
}

func (it *histogramIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	if value.IsStaleNaN(it.sum) {
		return it.t, &histogram.FloatHistogram{Sum: it.sum}
	}
	it.atFloatHistogramCalled = true
	return it.t, &histogram.FloatHistogram{
		Count:           float64(it.cnt),
		ZeroCount:       float64(it.zCnt),
		Sum:             it.sum,
		ZeroThreshold:   it.zThreshold,
		Schema:          it.schema,
		PositiveSpans:   it.pSpans,
		NegativeSpans:   it.nSpans,
		PositiveBuckets: it.pFloatBuckets,
		NegativeBuckets: it.nFloatBuckets,
	}
}

func (it *histogramIterator) AtT() int64 {
	return it.t
}

func (it *histogramIterator) Err() error {
	return it.err
}

func (it *histogramIterator) Reset(b []byte) {
	// The first 3 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[3:])
	it.numTotal = binary.BigEndian.Uint16(b)
	it.numRead = 0

	it.t, it.cnt, it.zCnt = 0, 0, 0
	it.tDelta, it.cntDelta, it.zCntDelta = 0, 0, 0

	// Recycle slices that have not been returned yet. Otherwise, start from
	// scratch.
	if it.atHistogramCalled {
		it.atHistogramCalled = false
		it.pBuckets, it.nBuckets = nil, nil
	} else {
		it.pBuckets = it.pBuckets[:0]
		it.nBuckets = it.nBuckets[:0]
	}
	if it.atFloatHistogramCalled {
		it.atFloatHistogramCalled = false
		it.pFloatBuckets, it.nFloatBuckets = nil, nil
	} else {
		it.pFloatBuckets = it.pFloatBuckets[:0]
		it.nFloatBuckets = it.nFloatBuckets[:0]
	}

	it.pBucketsDelta = it.pBucketsDelta[:0]
	it.pBucketsDelta = it.pBucketsDelta[:0]

	it.sum = 0
	it.leading = 0
	it.trailing = 0
	it.err = nil
}

func (it *histogramIterator) Next() ValueType {
	if it.err != nil || it.numRead == it.numTotal {
		return ValNone
	}

	if it.numRead == 0 {
		// The first read is responsible for reading the chunk layout
		// and for initializing fields that depend on it. We give
		// counter reset info at chunk level, hence we discard it here.
		schema, zeroThreshold, posSpans, negSpans, err := readHistogramChunkLayout(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.schema = schema
		it.zThreshold = zeroThreshold
		it.pSpans, it.nSpans = posSpans, negSpans
		numPBuckets, numNBuckets := countSpans(posSpans), countSpans(negSpans)
		// Allocate bucket slices as needed, recycling existing slices
		// in case this iterator was reset and already has slices of a
		// sufficient capacity.
		if numPBuckets > 0 {
			if cap(it.pBuckets) < numPBuckets {
				it.pBuckets = make([]int64, numPBuckets)
				// If cap(it.pBuckets) isn't sufficient, neither is the cap of the others.
				it.pBucketsDelta = make([]int64, numPBuckets)
				it.pFloatBuckets = make([]float64, numPBuckets)
			} else {
				for i := 0; i < numPBuckets; i++ {
					it.pBuckets = append(it.pBuckets, 0)
					it.pBucketsDelta = append(it.pBucketsDelta, 0)
					it.pFloatBuckets = append(it.pFloatBuckets, 0)
				}
			}
		}
		if numNBuckets > 0 {
			if cap(it.nBuckets) < numNBuckets {
				it.nBuckets = make([]int64, numNBuckets)
				// If cap(it.nBuckets) isn't sufficient, neither is the cap of the others.
				it.nBucketsDelta = make([]int64, numNBuckets)
				it.nFloatBuckets = make([]float64, numNBuckets)
			} else {
				for i := 0; i < numNBuckets; i++ {
					it.nBuckets = append(it.nBuckets, 0)
					it.nBucketsDelta = append(it.nBucketsDelta, 0)
					it.pFloatBuckets = append(it.pFloatBuckets, 0)
				}
			}
		}

		// Now read the actual data.
		t, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.t = t

		cnt, err := readVarbitUint(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.cnt = cnt

		zcnt, err := readVarbitUint(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.zCnt = zcnt

		sum, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.sum = math.Float64frombits(sum)

		var current int64
		for i := range it.pBuckets {
			v, err := readVarbitInt(&it.br)
			if err != nil {
				it.err = err
				return ValNone
			}
			it.pBuckets[i] = v
			current += it.pBuckets[i]
			it.pFloatBuckets[i] = float64(current)
		}
		current = 0
		for i := range it.nBuckets {
			v, err := readVarbitInt(&it.br)
			if err != nil {
				it.err = err
				return ValNone
			}
			it.nBuckets[i] = v
			current += it.nBuckets[i]
			it.nFloatBuckets[i] = float64(current)
		}

		it.numRead++
		return ValHistogram
	}

	// The case for the 2nd sample with single deltas is implicitly handled correctly with the double delta code,
	// so we don't need a separate single delta logic for the 2nd sample.

	// Recycle bucket slices that have not been returned yet. Otherwise,
	// copy them.
	if it.atHistogramCalled {
		it.atHistogramCalled = false
		if len(it.pBuckets) > 0 {
			newBuckets := make([]int64, len(it.pBuckets))
			copy(newBuckets, it.pBuckets)
			it.pBuckets = newBuckets
		} else {
			it.pBuckets = nil
		}
		if len(it.nBuckets) > 0 {
			newBuckets := make([]int64, len(it.nBuckets))
			copy(newBuckets, it.nBuckets)
			it.nBuckets = newBuckets
		} else {
			it.nBuckets = nil
		}
	}
	// FloatBuckets are set from scratch, so simply create empty ones.
	if it.atFloatHistogramCalled {
		it.atFloatHistogramCalled = false
		if len(it.pFloatBuckets) > 0 {
			it.pFloatBuckets = make([]float64, len(it.pFloatBuckets))
		} else {
			it.pFloatBuckets = nil
		}
		if len(it.nFloatBuckets) > 0 {
			it.nFloatBuckets = make([]float64, len(it.nFloatBuckets))
		} else {
			it.nFloatBuckets = nil
		}
	}

	tDod, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}
	it.tDelta = it.tDelta + tDod
	it.t += it.tDelta

	cntDod, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}
	it.cntDelta = it.cntDelta + cntDod
	it.cnt = uint64(int64(it.cnt) + it.cntDelta)

	zcntDod, err := readVarbitInt(&it.br)
	if err != nil {
		it.err = err
		return ValNone
	}
	it.zCntDelta = it.zCntDelta + zcntDod
	it.zCnt = uint64(int64(it.zCnt) + it.zCntDelta)

	ok := it.readSum()
	if !ok {
		return ValNone
	}

	if value.IsStaleNaN(it.sum) {
		it.numRead++
		return ValHistogram
	}

	var current int64
	for i := range it.pBuckets {
		dod, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.pBucketsDelta[i] += dod
		it.pBuckets[i] += it.pBucketsDelta[i]
		current += it.pBuckets[i]
		it.pFloatBuckets[i] = float64(current)
	}

	current = 0
	for i := range it.nBuckets {
		dod, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.nBucketsDelta[i] += dod
		it.nBuckets[i] += it.nBucketsDelta[i]
		current += it.nBuckets[i]
		it.nFloatBuckets[i] = float64(current)
	}

	it.numRead++
	return ValHistogram
}

func (it *histogramIterator) readSum() bool {
	err := xorRead(&it.br, &it.sum, &it.leading, &it.trailing)
	if err != nil {
		it.err = err
		return false
	}
	return true
}
