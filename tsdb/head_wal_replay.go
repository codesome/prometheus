// Copyright The Prometheus Authors
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

package tsdb

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

func (h *Head) registerReplayAppender() bool {
	select {
	case <-h.walReplayDone:
		return false
	default:
	}
	h.replayAppendersMtx.Lock()
	if h.replayMerging {
		h.replayAppendersMtx.Unlock()
		<-h.walReplayDone
		return false
	}
	h.replayAppenders++
	h.replayAppendersMtx.Unlock()
	return true
}

func (a *headAppenderBase) releaseReplayAppender() {
	if !a.replayRegistered {
		return
	}
	a.replayRegistered = false
	h := a.head
	h.replayAppendersMtx.Lock()
	h.replayAppenders--
	if h.replayMerging && h.replayAppenders == 0 {
		close(h.replayAppendersDrained)
	}
	h.replayAppendersMtx.Unlock()
}

func (h *Head) drainReplayAppenders() error {
	h.replayAppendersMtx.Lock()
	h.replayMerging = true
	h.replayAppendersDrained = make(chan struct{})
	if h.replayAppenders == 0 {
		close(h.replayAppendersDrained)
	}
	h.replayAppendersMtx.Unlock()
	select {
	case <-h.replayAppendersDrained:
		return nil
	case <-h.walReplayCtx.Done():
		return h.walReplayCtx.Err()
	}
}

// walReplayGenerations belongs to the replay controller and survives segment
// boundaries. Workers see source series through the ID index, never this map.
// Separate sources are essential: their samples can interleave in WAL order.
type walReplayGenerations struct {
	groups         map[*memSeries][]*memSeries
	parents        map[chunks.HeadSeriesRef]*memSeries
	discarded      []*memSeries
	metadataSeries map[*memSeries]struct{}
}

func (g *walReplayGenerations) add(h *Head, target *stripeSeries, parent *memSeries, def record.RefSeries) *memSeries {
	if g.groups == nil {
		g.groups = make(map[*memSeries][]*memSeries)
		g.parents = make(map[chunks.HeadSeriesRef]*memSeries)
	}
	s := newMemSeries(parent.lset, def.Ref, parent.shardHash, h.opts.IsolationDisabled, false)
	s.replayOnly = true
	idx := uint64(s.ref) & uint64(target.size-1)
	target.locks[idx].Lock()
	target.series[idx][s.ref] = s
	target.locks[idx].Unlock()
	g.groups[parent] = append(g.groups[parent], s)
	g.parents[s.ref] = parent
	return s
}

// supersede preserves ordinary Series semantics: recreation after compaction
// replaces the whole preceding generation, including its concurrent sources.
func (g *walReplayGenerations) supersede(parent *memSeries, multiRef map[chunks.HeadSeriesRef]chunks.HeadSeriesRef) {
	for _, s := range g.groups[parent] {
		multiRef[s.ref] = parent.ref
		delete(g.parents, s.ref)
		g.discarded = append(g.discarded, s)
	}
	delete(g.groups, parent)
}

func (g *walReplayGenerations) finish(h *Head, target *stripeSeries, multiRef map[chunks.HeadSeriesRef]chunks.HeadSeriesRef, persistMetadata bool) error {
	for parent, sources := range g.groups {
		if target.getByID(parent.ref) != parent {
			continue
		}
		all := make([]*memSeries, 1, len(sources)+1)
		all[0] = parent
		all = append(all, sources...)
		slices.SortFunc(all, func(a, b *memSeries) int { return cmp.Compare(a.ref, b.ref) })
		for _, s := range all {
			h.updateWALExpiry(s.ref, s.maxTime())
			if s.ooo != nil {
				for _, c := range s.ooo.oooMmappedChunks {
					h.updateWALExpiry(s.ref, c.maxTime)
				}
			}
		}
		var oooChunks []*mmappedChunk
		for _, s := range all {
			if s.ooo != nil {
				oooChunks = append(oooChunks, s.ooo.oooMmappedChunks...)
			}
		}
		// The newest source wins equal timestamps, matching live ingestion at
		// the original fast startup. Fold from newest to oldest.
		merged := all[len(all)-1]
		for i := len(all) - 2; i >= 0; i-- {
			if err := h.mergeSeries(merged, all[i]); err != nil {
				return fmt.Errorf("reconcile WAL series %d: %w", parent.ref, err)
			}
		}
		if merged != parent {
			parent.adoptChunks(merged)
			// Subsequent appends use parent's WAL ref, whereas the adopted tail
			// can contain another source. It cannot serve as a replay cache.
			parent.uncached = true
		}
		if len(oooChunks) > 0 {
			// OOO chunk IDs follow mapper order, not sample timestamp order.
			slices.SortFunc(oooChunks, func(a, b *mmappedChunk) int { return cmp.Compare(a.ref, b.ref) })
			parent.ooo = &memSeriesOOOFields{oooMmappedChunks: oooChunks}
		}
		for _, s := range sources {
			multiRef[s.ref] = parent.ref
			removeReplaySource(target, s.ref)
		}
	}
	for _, s := range g.discarded {
		// Workers have stopped, so discarded source data can now be released.
		h.deleteSeriesByID(target, []chunks.HeadSeriesRef{s.ref})
	}
	if persistMetadata && !h.fastReplay {
		var pending []*memSeries
		for s := range g.metadataSeries {
			if target.getByID(s.ref) == s && s.needsMetadataWAL {
				pending = append(pending, s)
			}
		}
		return h.logReplayMetadata(pending)
	}
	return nil
}

// logReplayMetadata makes metadata independent of expiring source aliases.
// Ingestion must be stopped. A repaired WAL prefix must call this after repair,
// otherwise repair could discard the newly written records.
func (h *Head) logReplayMetadata(series []*memSeries) error {
	if len(series) == 0 || h.wal == nil {
		return nil
	}
	buf := h.getBytesBuffer()
	defer func() { h.putBytesBuffer(buf) }()
	enc := record.Encoder{}
	refs := make([]record.RefMetadata, 0, min(len(series), 5000))
	for len(series) > 0 {
		n := min(len(series), 5000)
		refs = refs[:0]
		for _, s := range series[:n] {
			refs = append(refs, record.RefMetadata{
				Ref: s.ref, Type: record.GetMetricType(s.meta.Type), Unit: s.meta.Unit, Help: s.meta.Help,
			})
		}
		buf = enc.Metadata(refs, buf[:0])
		if err := h.wal.Log(buf); err != nil {
			return fmt.Errorf("persist reconciled metadata: %w", err)
		}
		for _, s := range series[:n] {
			s.needsMetadataWAL = false
		}
		series = series[n:]
	}
	return nil
}

func removeReplaySource(target *stripeSeries, ref chunks.HeadSeriesRef) {
	idx := uint64(ref) & uint64(target.size-1)
	target.locks[idx].Lock()
	delete(target.series[idx], ref)
	target.locks[idx].Unlock()
}

// adoptChunks moves data without copying the mutex, identity or pending-commit
// state of either series. Callers must ensure there are no readers of the source.
func (s *memSeries) adoptChunks(from *memSeries) {
	s.mmappedChunks = from.mmappedChunks
	s.headChunks = from.headChunks
	s.firstChunkID = from.firstChunkID
	s.mmMaxTime = from.mmMaxTime
	s.nextAt = from.nextAt
	s.histogramChunkHasComputedEndTime = from.histogramChunkHasComputedEndTime
	s.lastValue = from.lastValue
	s.lastHistogramValue = from.lastHistogramValue
	s.lastFloatHistogramValue = from.lastFloatHistogramValue
	s.app = from.app
	s.txs = from.txs
}

func (h *Head) mergeSeries(live, hist *memSeries) error {
	liveIntervals, _ := h.tombstones.Get(storage.SeriesRef(live.ref))
	histIntervals, _ := h.tombstones.Get(storage.SeriesRef(hist.ref))
	staleBefore := live.staleCount() + hist.staleCount()
	if len(liveIntervals) == 0 && len(histIntervals) == 0 && !live.prependHistory(hist, h.chunkDiskMapper) {
		h.updateStaleCount(staleBefore, live.staleCount())
		return nil
	}
	// A boundary retry should not decode hours of disjoint historical chunks.
	// Keep the immutable prefix; only the overlapping tail needs recoding.
	// Replay sources have no tracked transactions, so their prefix is visible
	// to all future readers without adding transaction entries.
	prefix := 0
	if len(histIntervals) == 0 && (hist.txs == nil || hist.txs.txIDCount == 0) {
		for prefix < len(hist.mmappedChunks) && hist.mmappedChunks[prefix].maxTime < live.minTime() {
			prefix++
		}
	}
	li, err := newReplayMergeIterator(live, h.chunkDiskMapper, liveIntervals, 0)
	if err != nil {
		return err
	}
	hi, err := newReplayMergeIterator(hist, h.chunkDiskMapper, histIntervals, prefix)
	if err != nil {
		return err
	}
	merged := newMemSeries(live.lset, live.ref, live.shardHash, live.txs == nil, false)
	merged.uncached = true
	merged.mmappedChunks = slices.Clone(hist.mmappedChunks[:prefix])
	opts := chunkOpts{
		chunkDiskMapper: h.chunkDiskMapper,
		chunkRange:      h.chunkRange.Load(),
		samplesPerChunk: h.opts.SamplesPerChunk,
		useXOR2:         h.opts.EnableXOR2Encoding.Load(),
	}
	li.next()
	hi.next()
	var previous *replayMergeIterator
	for li.typ != chunkenc.ValNone || hi.typ != chunkenc.ValNone {
		if err := h.walReplayCtx.Err(); err != nil {
			return err
		}
		it := li
		if li.typ == chunkenc.ValNone || (hi.typ != chunkenc.ValNone && hi.it.AtT() < li.it.AtT()) {
			it = hi
		} else if hi.typ != chunkenc.ValNone && hi.it.AtT() == li.it.AtT() {
			// The live/newer generation wins conflicts, regardless of sample
			// type or record/worker ordering. Consume both transaction entries.
			hi.next()
		}
		st := it.it.AtST()
		opts.useXOR2 = opts.useXOR2 || st != 0
		var created bool
		switch it.typ {
		case chunkenc.ValFloat:
			t, v := it.it.At()
			_, created = merged.append(st, t, v, 0, opts)
		case chunkenc.ValHistogram:
			t, v := it.it.AtHistogram(nil)
			if previous != it && v.CounterResetHint != histogram.GaugeType {
				v.CounterResetHint = histogram.UnknownCounterReset
			}
			_, created = merged.appendHistogram(st, t, v, 0, opts)
		case chunkenc.ValFloatHistogram:
			t, v := it.it.AtFloatHistogram(nil)
			if previous != it && v.CounterResetHint != histogram.GaugeType {
				v.CounterResetHint = histogram.UnknownCounterReset
			}
			_, created = merged.appendFloatHistogram(st, t, v, 0, opts)
		}
		// The ring describes a suffix, including historical samples between
		// live transactions. Zero IDs make those samples visible to all reads.
		if merged.txs != nil && (it.appendID != 0 || merged.txs.txIDCount != 0) {
			merged.txs.add(it.appendID)
		}
		if created {
			merged.mmapChunks(h.chunkDiskMapper)
		}
		previous = it
		it.next()
	}
	if err := errors.Join(li.it.Err(), hi.it.Err()); err != nil {
		return err
	}
	oldChunks := live.chunkCount() + hist.chunkCount()
	newChunks := merged.chunkCount()
	live.adoptChunks(merged)
	live.uncached = true
	h.metrics.chunksCreated.Add(float64(newChunks - prefix))
	h.metrics.chunksRemoved.Add(float64(oldChunks - prefix))
	h.metrics.chunks.Add(float64(newChunks - oldChunks))
	h.updateStaleCount(staleBefore, live.staleCount())
	h.tombstones.DeleteTombstones(map[storage.SeriesRef]struct{}{
		storage.SeriesRef(live.ref): {},
		storage.SeriesRef(hist.ref): {},
	})
	return nil
}

func (s *memSeries) chunkCount() int {
	n := len(s.mmappedChunks)
	if s.headChunks != nil {
		n += s.headChunks.len()
	}
	return n
}

func (h *Head) updateStaleCount(before, after uint64) {
	if before < after {
		h.numStaleSeries.Add(after - before)
	} else if before > after {
		h.numStaleSeries.Sub(before - after)
	}
}

type replayMergeIterator struct {
	it        chunkenc.Iterator
	typ       chunkenc.ValueType
	txs       *txRingIterator
	untracked int
	appendID  uint64
	intervals tombstones.Intervals
}

func newReplayMergeIterator(s *memSeries, cdm *chunks.ChunkDiskMapper, intervals tombstones.Intervals, skipMmap int) (*replayMergeIterator, error) {
	cs := make([]chunkenc.Iterable, 0, s.chunkCount()-skipMmap)
	count := 0
	for _, mm := range s.mmappedChunks[skipMmap:] {
		c, err := cdm.Chunk(mm.ref)
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
		count += c.NumSamples()
	}
	start := len(cs)
	for c := s.headChunks; c != nil; c = c.prev {
		cs = append(cs, c.chunk)
		count += c.chunk.NumSamples()
	}
	slices.Reverse(cs[start:])
	it := &replayMergeIterator{it: storage.ChainSampleIteratorFromIterables(nil, cs), untracked: count, intervals: intervals}
	if s.txs != nil && s.txs.txIDCount != 0 {
		it.untracked -= int(s.txs.txIDCount)
		it.txs = s.txs.iterator()
	}
	return it, nil
}

func (it *replayMergeIterator) next() {
	for {
		it.typ = it.it.Next()
		if it.typ == chunkenc.ValNone {
			return
		}
		it.appendID = 0
		if it.untracked > 0 {
			it.untracked--
		} else if it.txs != nil {
			it.appendID = it.txs.At()
			it.txs.Next()
		}
		t := it.it.AtT()
		for len(it.intervals) > 0 && t > it.intervals[0].Maxt {
			it.intervals = it.intervals[1:]
		}
		if len(it.intervals) == 0 || !it.intervals[0].InBounds(t) {
			return
		}
	}
}
