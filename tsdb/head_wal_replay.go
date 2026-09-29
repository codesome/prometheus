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

	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

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
	// Parents replaced by a newer source, and the survivor replacing each.
	replaced := make(map[chunks.HeadSeriesRef]*memSeries)
	for parent, sources := range g.groups {
		if target.getByID(parent.ref) != parent {
			continue
		}
		all := make([]*memSeries, 1, len(sources)+1)
		all[0] = parent
		all = append(all, sources...)
		slices.SortFunc(all, func(a, b *memSeries) int { return cmp.Compare(a.ref, b.ref) })
		for _, s := range all {
			// Only one series survives the fold below; it is recounted afterwards.
			if s.headChunkCount.Load() >= 2 {
				target.decMmapReady(s.ref)
			}
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
			// The newest source survives rather than the parent. It owns its head
			// chunks and the latest data, so subsequent appends extend chunks it
			// owns, and the next replay folds older sources onto it without overlap.
			merged.replayOnly = false
			merged.meta = parent.meta
			if merged.meta != nil {
				// The metadata may have been recorded under a ref that will expire.
				merged.needsMetadataWAL = true
				if g.metadataSeries == nil {
					g.metadataSeries = make(map[*memSeries]struct{})
				}
				g.metadataSeries[merged] = struct{}{}
			}
			delete(g.metadataSeries, parent)
			hash := parent.lset.Hash()
			i := hash & uint64(target.size-1)
			target.locks[i].Lock()
			target.hashes[i].set(hash, merged)
			target.locks[i].Unlock()
			removeReplaySource(target, parent.ref)
			replaced[parent.ref] = merged
		}
		if merged.headChunkCount.Load() >= 2 {
			target.incMmapReady(merged.ref)
		}
		if len(oooChunks) > 0 {
			// OOO chunk IDs follow mapper order, not sample timestamp order.
			slices.SortFunc(oooChunks, func(a, b *mmappedChunk) int { return cmp.Compare(a.ref, b.ref) })
			merged.ooo = &memSeriesOOOFields{oooMmappedChunks: oooChunks}
		}
		for _, s := range all {
			if s != merged && s != parent {
				multiRef[s.ref] = merged.ref
				removeReplaySource(target, s.ref)
			}
		}
	}
	if len(replaced) > 0 {
		// Aliases of a replaced parent, including its own ref, now resolve to the survivor.
		for ref, to := range multiRef {
			if s := replaced[to]; s != nil {
				multiRef[ref] = s.ref
			}
		}
		for ref, s := range replaced {
			multiRef[ref] = s.ref
		}
		if target == h.series {
			deleted := make(map[storage.SeriesRef]struct{}, len(replaced))
			affected := make(map[labels.Label]struct{})
			for ref, s := range replaced {
				deleted[storage.SeriesRef(ref)] = struct{}{}
				s.lset.Range(func(l labels.Label) { affected[l] = struct{}{} })
			}
			h.postings.Delete(deleted, affected)
			for _, s := range replaced {
				h.postings.Add(storage.SeriesRef(s.ref), s.lset)
			}
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
// It may run concurrently with ingestion. Each record is logged while its series
// are locked, so that a later metadata update is logged after it, and a series
// with an open transaction is retried once that finishes. A repaired WAL prefix
// must call this after repair, otherwise repair could discard the new records.
func (h *Head) logReplayMetadata(series []*memSeries) error {
	if len(series) == 0 || h.wal == nil {
		return nil
	}
	// Locking a series twice below would deadlock.
	slices.SortFunc(series, func(a, b *memSeries) int { return cmp.Compare(a.ref, b.ref) })
	series = slices.Compact(series)
	buf := h.getBytesBuffer()
	defer func() { h.putBytesBuffer(buf) }()
	enc := record.Encoder{}
	refs := make([]record.RefMetadata, 0, min(len(series), 5000))
	logged := make([]*memSeries, 0, cap(refs))
	var busy []*memSeries
	for len(series) > 0 {
		batch := series[:min(len(series), 5000)]
		series = series[len(batch):]
		refs, logged = refs[:0], logged[:0]
		for _, s := range batch {
			s.Lock()
			switch {
			case !s.needsMetadataWAL:
				// An appender has committed newer metadata since.
			case s.hasPendingCommit():
				// An appender may have logged newer metadata without applying it yet.
				busy = append(busy, s)
			default:
				refs = append(refs, record.RefMetadata{
					Ref: s.ref, Type: record.GetMetricType(s.meta.Type), Unit: s.meta.Unit, Help: s.meta.Help,
				})
				logged = append(logged, s)
			}
		}
		var err error
		if len(refs) > 0 {
			buf = enc.Metadata(refs, buf[:0])
			err = h.wal.Log(buf)
		}
		if err == nil {
			for _, s := range logged {
				s.needsMetadataWAL = false
			}
		}
		for _, s := range batch {
			s.Unlock()
		}
		if err != nil {
			return fmt.Errorf("persist reconciled metadata: %w", err)
		}
		if len(series) == 0 && len(busy) > 0 {
			if err := h.waitReplayRetry(); err != nil {
				return err
			}
			series, busy = busy, nil
		}
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
	s.setHeadChunks(from.headChunks, from.headChunkCount.Load())
	s.firstChunkID = from.firstChunkID
	s.mmMaxTime = from.mmMaxTime
	s.nextAt = from.nextAt
	s.setComputedHistogramChunkEndTime(from.hasComputedHistogramChunkEndTime())
	s.lastValue = from.lastValue
	s.lastHistogramValue = from.lastHistogramValue
	s.lastFloatHistogramValue = from.lastFloatHistogramValue
	s.app = from.app
	s.txs = from.txs
}

func (h *Head) mergeSeries(live, hist *memSeries) error {
	liveIntervals, _ := h.tombstones.Get(storage.SeriesRef(live.ref))
	histIntervals, _ := h.tombstones.Get(storage.SeriesRef(hist.ref))
	gaugesBefore := sampleGauges{}.add(live).add(hist)
	if len(liveIntervals) == 0 && len(histIntervals) == 0 && !live.prependHistory(hist, h.chunkDiskMapper) {
		h.updateSampleGauges(gaugesBefore, sampleGauges{}.add(live))
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
		useXOR2:         h.opts.UseXOR2FloatEncoding(),
		useHistogramST:  h.opts.EnableHistogramSTEncoding.Load(),
		storeST:         h.opts.EnableSTStorage.Load(),
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
		if st != 0 {
			// Keep start timestamps already stored in either source.
			opts.useXOR2, opts.useHistogramST = true, true
		}
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
	h.updateSampleGauges(gaugesBefore, sampleGauges{}.add(live))
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

// sampleGauges sums series contributions to the gauges derived from the latest
// in-order sample: stale series, native histogram series and their buckets.
type sampleGauges struct{ stale, histograms, buckets int }

func (g sampleGauges) add(s *memSeries) sampleGauges {
	stale, isHist, buckets := s.sampleState()
	if stale {
		g.stale++
	}
	if isHist {
		g.histograms++
		g.buckets += buckets
	}
	return g
}

// updateSampleGauges replaces the contributions before with after.
func (h *Head) updateSampleGauges(before, after sampleGauges) {
	addToGauge(&h.numStaleSeries, after.stale-before.stale)
	addToGauge(&h.numNativeHistogramSeries, after.histograms-before.histograms)
	h.addNativeHistogramBuckets(after.buckets - before.buckets)
}

func addToGauge(g *atomic.Uint64, delta int) {
	if delta >= 0 {
		g.Add(uint64(delta))
		return
	}
	g.Sub(uint64(-delta))
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
