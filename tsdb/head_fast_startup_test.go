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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

type fastStartupReplayGate struct {
	entered, release chan struct{}
	blocked          atomic.Bool
	err              error
}

func (g *fastStartupReplayGate) PreCreation(labels.Labels) error {
	if g.blocked.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
		return g.err
	}
	return nil
}
func (*fastStartupReplayGate) PostCreation(labels.Labels)                          {}
func (*fastStartupReplayGate) PostDeletion(map[chunks.HeadSeriesRef]labels.Labels) {}

func newFastStartupTestHead(t *testing.T, dir string, fast bool, cb SeriesLifecycleCallback) *Head {
	t.Helper()
	opts := DefaultHeadOptions()
	opts.ChunkDirRoot = dir
	opts.ChunkRange = 1000
	opts.StripeSize = 16
	opts.EnableFastStartup = fast
	opts.SeriesCallback = cb
	w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32768, compression.None)
	require.NoError(t, err)
	h, err := NewHead(nil, nil, w, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func restartFastStartupPaused(t *testing.T, dir string) (*Head, func()) {
	t.Helper()
	g := &fastStartupReplayGate{entered: make(chan struct{}), release: make(chan struct{})}
	h := newFastStartupTestHead(t, dir, true, g)
	require.NoError(t, h.InitFastStartup(0))
	<-g.entered
	var once sync.Once
	resume := func() { once.Do(func() { close(g.release) }); <-h.WaitForWALReplay() }
	t.Cleanup(resume)
	return h, resume
}

func fastStartupSamples(t *testing.T, h *Head, name string) []chunks.Sample {
	t.Helper()
	q, err := NewBlockQuerier(h, 0, 10000)
	require.NoError(t, err)
	defer q.Close()
	return query(t, q, labels.MustNewMatcher(labels.MatchEqual, "__name__", name))[fmt.Sprintf("{__name__=%q}", name)]
}

func TestHeadFastStartupMissingState(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	newLabels := labels.FromStrings("__name__", "new")
	fastStartupAppend(t, h2, newLabels, 1000)
	ref := h2.series.getByHash(newLabels.Hash(), newLabels).ref
	resume()
	require.Equal(t, newLabels, h2.series.getByID(ref).labels(), "The historical ref must not overwrite a different live series")
}

func TestHeadFastStartupSlowPathChunkOrder(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, rangeSlice(1000, 1400)...)
	h2.mmapHeadChunks()
	require.NotEmpty(t, h2.series.getByHash(lset.Hash(), lset).mmappedChunks)
	resume()
	require.Len(t, fastStartupSamples(t, h2, "m"), 402)
	require.NoError(t, h2.Close())
	h3 := newFastStartupTestHead(t, dir, false, nil)
	_, _, _, err := h3.loadMmappedChunks(map[chunks.HeadSeriesRef]*memSeries{})
	require.NoError(t, err, "The merge must preserve per-ref chronological ordering on disk")
}

func TestHeadFastStartupTombstoneRemap(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Delete(context.Background(), 100, 100, labels.MustNewMatcher(labels.MatchEqual, "__name__", "m")))
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 1000)
	resume()
	samples := fastStartupSamples(t, h2, "m")
	require.Len(t, samples, 2, "The deleted historical sample must not reappear under the live ref")
}

func TestHeadFastStartupStaleTombstone(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, lset, 100, 200)
	s := h1.series.getByHash(lset.Hash(), lset)
	var enc record.Encoder
	require.NoError(t, h1.wal.Log(enc.Tombstones([]tombstones.Stone{{Ref: 1, Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: math.MaxInt64}}}}, nil)))
	require.Equal(t, chunks.HeadSeriesRef(1), s.ref)
	require.NoError(t, h1.Close())
	h2 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h2.InitFastStartup(0))
	<-h2.WaitForWALReplay()
	require.Empty(t, fastStartupSamples(t, h2, "m"), "A fully deleted historical series must not be merged back")
}

func TestHeadFastStartupCheckpointRetention(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, lset, 100, 200)
	oldRef := h1.series.getByHash(lset.Hash(), lset).ref
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 1000)
	resume()
	require.True(t, h2.keepSeriesInWALCheckpointFn(150)(oldRef), "Uncompacted historical samples still need the old series record")
}

func TestHeadFastStartupIDScan(t *testing.T) {
	h := newFastStartupTestHead(t, t.TempDir(), false, nil)
	require.NoError(t, h.Init(0))
	var enc record.Encoder
	require.NoError(t, h.wal.Log(enc.Series([]record.RefSeries{{Ref: 100, Labels: labels.FromStrings("__name__", "high")}}, nil)))
	_, err := h.wal.NextSegment()
	require.NoError(t, err)
	require.NoError(t, h.wal.Log(enc.Series([]record.RefSeries{{Ref: 99, Labels: labels.FromStrings("__name__", "low")}}, nil)))
	_, err = h.wal.NextSegment()
	require.NoError(t, err)
	id, err := h.findLastSeriesID(SeriesLifecycleState{LastSeriesID: 100, LastWALSegment: 0}, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, id, uint64(100), "Commit order need not match series-ID allocation order")
}

func TestHeadFastStartupIDScanCheckpoint(t *testing.T) {
	h := newFastStartupTestHead(t, t.TempDir(), false, nil)
	require.NoError(t, h.Init(0))
	var enc record.Encoder
	require.NoError(t, h.wal.Log(enc.Series([]record.RefSeries{{Ref: 100, Labels: labels.FromStrings("__name__", "checkpoint")}}, nil)))
	_, err := h.wal.NextSegment()
	require.NoError(t, err)
	_, err = wlog.Checkpoint(h.logger, h.wal, 0, 0, func(chunks.HeadSeriesRef) bool { return true }, 0, false, true)
	require.NoError(t, err)
	require.NoError(t, h.wal.Truncate(1))
	id, err := h.findLastSeriesID(SeriesLifecycleState{}, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(100), id, "the checkpoint can contain the highest surviving reference")
}

func TestHeadFastStartupStaleCleanState(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), 100)
	require.NoError(t, h1.writeSeriesState(true))
	// A run with fast startup disabled does not maintain the state hint.
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "newer"), 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	require.GreaterOrEqual(t, h2.lastSeriesID.Load(), uint64(2))
	resume()
	require.NoError(t, h2.WALReplayError())
}

func TestHeadFastStartupFallbacks(t *testing.T) {
	for _, mode := range []string{"exemplars", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			opts := newTestHeadDefaultOptions(1000, false)
			opts.EnableFastStartup = true
			opts.EnableExemplarStorage = mode == "exemplars"
			opts.EnableMemorySnapshotOnShutdown = mode == "snapshot"
			opts.MaxExemplars.Store(100)
			h1, w := newTestHeadWithOptions(t, compression.None, opts)
			require.NoError(t, h1.InitFastStartup(0))
			require.False(t, h1.fastReplay)
			lset := labels.FromStrings("__name__", "m")
			appendSample := func(h *Head, ts int64) {
				a := h.Appender(context.Background())
				ref, err := a.Append(0, lset, ts, float64(ts))
				require.NoError(t, err)
				if mode == "exemplars" {
					_, err = a.AppendExemplar(ref, lset, exemplar.Exemplar{Ts: ts, HasTs: true, Value: float64(ts), Labels: labels.FromStrings("traceID", strconv.FormatInt(ts, 10))})
					require.NoError(t, err)
				}
				require.NoError(t, a.Commit())
			}
			appendSample(h1, 100)
			require.NoError(t, h1.Close())
			wal, err := wlog.NewSize(nil, nil, w.Dir(), 32768, compression.None)
			require.NoError(t, err)
			h2, err := NewHead(nil, nil, wal, nil, opts, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h2.Close()) })
			require.NoError(t, h2.InitFastStartup(0))
			require.False(t, h2.fastReplay)
			appendSample(h2, 1000)
			require.Len(t, fastStartupSamples(t, h2, "m"), 2)
			if mode == "exemplars" {
				q, err := h2.ExemplarQuerier(context.Background())
				require.NoError(t, err)
				results, err := q.Select(0, 2000, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "m")})
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Len(t, results[0].Exemplars, 2)
			}
		})
	}
}

func TestHeadFastStartupConcurrentNewSeries(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), 100)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 20000 {
				lset := labels.FromStrings("__name__", fmt.Sprintf("live_%d_%d", i, j))
				_, _, err := h2.getOrCreate(lset.Hash(), lset, true)
				if err != nil {
					panic(err)
				}
			}
		}(i)
	}
	resume()
	wg.Wait()
}

func TestHeadFastStartupStaleCount(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, lset, 100)
	a := h1.Appender(context.Background())
	_, err := a.Append(0, lset, 200, math.Float64frombits(value.StaleNaN))
	require.NoError(t, err)
	require.NoError(t, a.Commit())
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 1000)
	resume()
	require.Zero(t, h2.NumStaleSeries(), "The surviving live series is not stale")
}

func TestHeadFastStartupExistingWBL(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, false, nil)
	wbl, err := wlog.NewSize(nil, nil, filepath.Join(dir, wlog.WblDirName), 32768, compression.None)
	require.NoError(t, err)
	require.NoError(t, h1.SetOutOfOrderTimeWindow(1000, wbl))
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, lset, 200)
	fastStartupAppend(t, h1, lset, 100)
	require.NoError(t, h1.Close())
	h2 := newFastStartupTestHead(t, dir, true, nil)
	h2.wbl, err = wlog.NewSize(nil, nil, filepath.Join(dir, wlog.WblDirName), 32768, compression.None)
	require.NoError(t, err)
	require.NoError(t, h2.InitFastStartup(0))
	<-h2.WaitForWALReplay()
	s := h2.series.getByHash(lset.Hash(), lset)
	require.NotNil(t, s)
	require.NotNil(t, s.ooo, "Disabling future OOO ingestion must not discard existing WBL data")
}

func TestHeadFastStartupMetadata(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	a := h1.Appender(context.Background())
	ref, err := a.Append(0, lset, 100, 100)
	require.NoError(t, err)
	meta := metadata.Metadata{Type: model.MetricTypeCounter, Unit: "seconds", Help: "Duration"}
	_, err = a.UpdateMetadata(ref, lset, meta)
	require.NoError(t, err)
	require.NoError(t, a.Commit())
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 1000)
	resume()
	require.Equal(t, &meta, h2.series.getByHash(lset.Hash(), lset).meta, "Historical metadata must survive when live ingestion has no replacement")
}

func TestHeadFastStartupChunkGC(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, true, nil)
	require.NoError(t, h1.InitFastStartup(0))
	<-h1.WaitForWALReplay()
	fastStartupAppend(t, h1, lset, 900, 1100)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, rangeSlice(2000, 2500)...)
	h2.mmapHeadChunks()
	liveS := h2.series.getByHash(lset.Hash(), lset)
	require.NotEmpty(t, liveS.mmappedChunks)
	liveChunkRef := liveS.mmappedChunks[0].ref
	h2.chunkDiskMapper.CutNewFile()
	resume()
	require.NoError(t, h2.Truncate(1000))
	_, err := h2.chunkDiskMapper.Chunk(liveChunkRef)
	require.NoError(t, err, "GC must not delete newer live chunks because the older historical chunk was written to a later file")
}

func TestHeadFastStartupReplayFailure(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), 100)
	require.NoError(t, h1.Close())

	wantErr := errors.New("series callback rejected replay")
	gate := &fastStartupReplayGate{entered: make(chan struct{}), release: make(chan struct{}), err: wantErr}
	h2 := newFastStartupTestHead(t, dir, true, gate)
	require.NoError(t, h2.InitFastStartup(0))
	<-gate.entered
	var once sync.Once
	resume := func() { once.Do(func() { close(gate.release) }); <-h2.WaitForWALReplay() }
	t.Cleanup(resume)

	db := &DB{head: h2}
	require.ErrorIs(t, h2.WALReplayError(), ErrNotReady)
	_, err := db.Querier(0, 2000)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = db.ChunkQuerier(0, 2000)
	require.ErrorIs(t, err, ErrNotReady)
	_, err = db.ExemplarQuerier(context.Background())
	require.ErrorIs(t, err, ErrNotReady)
	require.ErrorIs(t, db.Compact(context.Background()), ErrNotReady)
	require.ErrorIs(t, db.CompactStaleHead(), ErrNotReady)
	require.ErrorIs(t, db.Snapshot(t.TempDir(), true), ErrNotReady)
	require.ErrorIs(t, db.Delete(context.Background(), 0, 2000), ErrNotReady)
	require.ErrorIs(t, h2.Truncate(200), ErrNotReady)
	require.Error(t, db.ApplyConfig(&config.Config{StorageConfig: config.StorageConfig{
		TSDBConfig: &config.TSDBConfig{OutOfOrderTimeWindow: 1000},
	}}))

	// A failed replay must not be mistaken for readiness, and must not erase
	// the unrelated series whose ingestion has already been acknowledged.
	liveLabels := labels.FromStrings("__name__", "live")
	fastStartupAppend(t, h2, liveLabels, 1000)
	resume()
	require.ErrorIs(t, h2.WALReplayError(), wantErr)
	_, err = db.Querier(0, 2000)
	require.ErrorIs(t, err, wantErr)
	require.NotNil(t, h2.series.getByHash(liveLabels.Hash(), liveLabels))
	require.False(t, h2.compactable())
	// An in-flight scrape can create series after the failure was published,
	// before the server stops ingestion. It must not supersede unreplayed data.
	fastStartupAppend(t, h2, labels.FromStrings("__name__", "old"), 1100)
	require.NoError(t, h2.Close())
	h3 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h3.Init(0))
	require.Len(t, fastStartupSamples(t, h3, "old"), 2)
}

func TestHeadFastStartupCloseCancelsReplay(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), rangeSlice(0, 400)...)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	closed := make(chan error, 1)
	go func() { closed <- h2.Close() }()
	select {
	case <-h2.walReplayCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel replay")
	}
	resume()
	require.NoError(t, <-closed)
	require.ErrorIs(t, h2.WALReplayError(), context.Canceled)
	state, err := h2.readSeriesStateFile()
	require.NoError(t, err)
	require.False(t, state.CleanShutdown)
}

type fastStartupRecoveryGate struct {
	slog.Handler
	entered, release chan struct{}
}

func (*fastStartupRecoveryGate) Enabled(context.Context, slog.Level) bool { return true }

func (g *fastStartupRecoveryGate) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "Loading on-disk chunks failed" {
		close(g.entered)
		<-g.release
	}
	return nil
}

func TestHeadFastStartupRecoveryBeforeIngestion(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), rangeSlice(0, 400)...)
	require.NoError(t, h1.Close())
	files, err := os.ReadDir(filepath.Join(dir, "chunks_head"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	f, err := os.OpenFile(filepath.Join(dir, "chunks_head", files[0].Name()), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xff}, int64(chunks.HeadChunkFileHeaderSize+chunks.SeriesRefSize))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	h2 := newFastStartupTestHead(t, dir, true, nil)
	gate := &fastStartupRecoveryGate{Handler: slog.DiscardHandler, entered: make(chan struct{}), release: make(chan struct{})}
	h2.logger = slog.New(gate)
	started := make(chan error, 1)
	go func() { started <- h2.InitFastStartup(0) }()
	<-gate.entered
	var once sync.Once
	resume := func() { once.Do(func() { close(gate.release) }) }
	t.Cleanup(resume)
	select {
	case <-started:
		t.Fatal("ingestion was allowed before mmap recovery completed")
	default:
	}
	resume()
	require.NoError(t, <-started)
	fastStartupAppend(t, h2, labels.FromStrings("__name__", "live"), 1000)
	<-h2.WaitForWALReplay()
	require.NoError(t, h2.WALReplayError())
	require.Len(t, fastStartupSamples(t, h2, "old"), 400)
	require.Len(t, fastStartupSamples(t, h2, "live"), 1)
}

func TestHeadFastStartupOverlap(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 200, 300)
	resume()
	require.NoError(t, h2.WALReplayError())
	require.Len(t, fastStartupSamples(t, h2, "m"), 3)
	require.NoError(t, h2.Close())
	h3 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h3.Init(0))
	require.Len(t, fastStartupSamples(t, h3, "m"), 3)
}

func TestHeadFastStartupGenerationRecovery(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap=%v", overlap), func(t *testing.T) {
			dir := t.TempDir()
			lset := labels.FromStrings("__name__", "m")
			want := make(map[int64]float64)
			appendSamples := func(h *Head, start, end, step int64, offset float64) {
				a := h.Appender(context.Background())
				for ts := start; ts < end; ts += step {
					v := float64(ts) + offset
					_, err := a.Append(0, lset, ts, v)
					require.NoError(t, err)
					want[ts] = v
				}
				require.NoError(t, a.Commit())
			}
			check := func(h *Head) {
				got := fastStartupSamples(t, h, "m")
				times := make([]int64, 0, len(want))
				for ts := range want {
					times = append(times, ts)
				}
				slices.Sort(times)
				require.Len(t, got, len(times))
				for i, ts := range times {
					require.Equal(t, ts, got[i].T())
					require.Equal(t, want[ts], got[i].F())
				}
			}
			h1 := newFastStartupTestHead(t, dir, false, nil)
			require.NoError(t, h1.Init(0))
			appendSamples(h1, 0, 400, 2, 0)
			require.NoError(t, h1.Close())
			h2, resume := restartFastStartupPaused(t, dir)
			if overlap {
				appendSamples(h2, 1, 201, 2, 1000)
				appendSamples(h2, 200, 400, 1, 1000)
			} else {
				appendSamples(h2, 1000, 1400, 1, 1000)
			}
			h2.mmapHeadChunks()
			resume()
			require.NoError(t, h2.WALReplayError())
			check(h2)
			require.NoError(t, h2.Close())
			for restart := range 3 {
				h := newFastStartupTestHead(t, dir, false, nil)
				require.NoError(t, h.Init(0))
				check(h)
				appendSamples(h, 2000+int64(restart)*500, 2400+int64(restart)*500, 1, 0)
				h.mmapHeadChunks()
				first, _, err := wlog.Segments(h.wal.Dir())
				require.NoError(t, err)
				next, err := h.wal.NextSegment()
				require.NoError(t, err)
				_, err = wlog.Checkpoint(h.logger, h.wal, first, next-1, h.keepSeriesInWALCheckpointFn(0), 0, false, true)
				require.NoError(t, err)
				require.NoError(t, h.Close())
			}
		})
	}
}

func TestHeadFastStartupDrainsTransactions(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	a := h2.Appender(context.Background())
	_, err := a.Append(0, lset, 150, 3)
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		resume()
		close(done)
	}()
	require.Eventually(t, func() bool {
		h2.replayAppendersMtx.Lock()
		defer h2.replayAppendersMtx.Unlock()
		return h2.replayMerging
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, h2.WALReplayError(), ErrNotReady)
	require.NoError(t, a.Commit())
	<-done
	require.NoError(t, h2.WALReplayError())
	require.Len(t, fastStartupSamples(t, h2, "m"), 3)
	require.NoError(t, h2.Close())
	h3 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h3.Init(0))
	require.Len(t, fastStartupSamples(t, h3, "m"), 3)
}

func BenchmarkHeadFastStartupMerge(b *testing.B) {
	for _, count := range []int{10000, 20000, 40000} {
		b.Run(fmt.Sprintf("series=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				opts := DefaultHeadOptions()
				opts.ChunkDirRoot = b.TempDir()
				opts.EnableFastStartup = true
				opts.StripeSize = 1024
				opts.WALReplayConcurrency = 1
				h, err := NewHead(nil, nil, nil, nil, opts, nil)
				require.NoError(b, err)
				// Replay leaves postings unordered until the merge completes.
				for i := 1; i <= count; i++ {
					lset := labels.FromStrings("__name__", "m", "instance", strconv.Itoa(i))
					_, _, err := h.getOrCreateInStripe(h.walSeries, chunks.HeadSeriesRef(i), lset.Hash(), lset, false)
					require.NoError(b, err)
				}
				b.StartTimer()
				err = h.mergeWALSeries()
				b.StopTimer()
				require.NoError(b, err)
				require.Equal(b, uint64(count), h.NumSeries())
				p := h.postings.All()
				var previous storage.SeriesRef
				for p.Next() {
					require.Greater(b, p.At(), previous)
					previous = p.At()
				}
				require.NoError(b, p.Err())
				require.NoError(b, h.Close())
			}
		})
	}
}

// BenchmarkHeadStartup includes ID scanning, WAL decoding, chunk building, a
// first scrape of every series, and the final merge. The input has no chunk
// cache or allocator hint, as after a crash or first enabling the feature.
func BenchmarkHeadStartup(b *testing.B) {
	const samplesPerSeries = 240
	for _, seriesCount := range []int{10000, 100000, 1000000} {
		for _, fast := range []bool{false, true} {
			b.Run(fmt.Sprintf("series=%d/fast=%v", seriesCount, fast), func(b *testing.B) {
				var firstScrape, ready time.Duration
				for range b.N {
					b.StopTimer()
					dir := b.TempDir()
					w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), wlog.DefaultSegmentSize, compression.Snappy)
					require.NoError(b, err)
					defs := make([]record.RefSeries, seriesCount)
					for i := range defs {
						defs[i] = record.RefSeries{Ref: chunks.HeadSeriesRef(i + 1), Labels: labels.FromStrings("__name__", "m", "instance", strconv.Itoa(i))}
					}
					enc := record.Encoder{}
					require.NoError(b, w.Log(enc.Series(defs, nil)))
					ss := make([]record.RefSample, seriesCount)
					var buf []byte
					for ts := range samplesPerSeries {
						for i := range ss {
							ss[i] = record.RefSample{Ref: defs[i].Ref, T: int64(ts) * 15000, V: float64(ts + i)}
						}
						buf = enc.Samples(ss, buf[:0])
						require.NoError(b, w.Log(buf))
					}
					require.NoError(b, w.Close())
					w, err = wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), wlog.DefaultSegmentSize, compression.Snappy)
					require.NoError(b, err)
					opts := DefaultHeadOptions()
					opts.ChunkDirRoot = dir
					opts.EnableFastStartup = fast
					opts.EnableExemplarStorage = false
					opts.WALReplayConcurrency = 4
					h, err := NewHead(nil, nil, w, nil, opts, nil)
					require.NoError(b, err)
					b.StartTimer()
					start := time.Now()
					require.NoError(b, h.InitFastStartup(0))
					a := h.Appender(context.Background())
					for _, s := range defs {
						_, err := a.Append(0, s.Labels, samplesPerSeries*15000, 1)
						require.NoError(b, err)
					}
					require.NoError(b, a.Commit())
					firstScrape += time.Since(start)
					<-h.WaitForWALReplay()
					require.NoError(b, h.WALReplayError())
					ready += time.Since(start)
					b.StopTimer()
					require.NoError(b, h.Close())
				}
				b.ReportMetric(float64(firstScrape.Nanoseconds())/float64(b.N), "first-scrape-ns/op")
				b.ReportMetric(float64(ready.Nanoseconds())/float64(b.N), "ready-ns/op")
			})
		}
	}
}
