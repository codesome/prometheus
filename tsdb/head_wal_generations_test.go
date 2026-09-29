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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

func TestHeadConcurrentWALGenerations(t *testing.T) {
	enc := record.Encoder{}
	lset := labels.FromStrings("__name__", "m")
	series := func(ref chunks.HeadSeriesRef, concurrent bool) []byte {
		defs := []record.RefSeries{{Ref: ref, Labels: lset}}
		if concurrent {
			return enc.ConcurrentSeries(defs, nil)
		}
		return enc.Series(defs, nil)
	}
	samples := func(ref chunks.HeadSeriesRef, ts ...int64) []byte {
		var ss []record.RefSample
		for _, t := range ts {
			ss = append(ss, record.RefSample{Ref: ref, T: t, V: float64(t)})
		}
		return enc.Samples(ss, nil)
	}
	deleted := func(ref storage.SeriesRef, mint, maxt int64) []byte {
		return enc.Tombstones([]tombstones.Stone{{Ref: ref, Intervals: tombstones.Intervals{{Mint: mint, Maxt: maxt}}}}, nil)
	}
	for name, tc := range map[string]struct {
		records [][]byte
		want    []int64
	}{
		"interleaved sources": {
			records: [][]byte{series(1, false), samples(1, 100), series(2, true), samples(2, 200), samples(1, 300)},
			want:    []int64{100, 200, 300},
		},
		"repeated concurrent definition": {
			records: [][]byte{series(1, false), samples(1, 100), series(2, true), samples(2, 200), series(2, true), samples(2, 300)},
			want:    []int64{100, 200, 300},
		},
		"normal recreation supersedes concurrent sources": {
			records: [][]byte{series(1, false), samples(1, 100), series(2, true), samples(2, 200), series(3, false), samples(3, 300)},
			want:    []int64{300},
		},
		"delete predates concurrent source": {
			records: [][]byte{series(1, false), samples(1, 100), deleted(1, 0, 1000), series(2, true), samples(2, 100, 200)},
			want:    []int64{100, 200},
		},
		"delete follows concurrent source": {
			records: [][]byte{series(1, false), samples(1, 100), series(2, true), samples(2, 200, 300), deleted(2, 0, 200)},
			want:    []int64{300},
		},
		"full deletion and recreation": {
			records: [][]byte{series(1, false), samples(1, 100), series(2, true), samples(2, 200), deleted(2, math.MinInt64, math.MaxInt64), series(3, false), samples(3, 300)},
			want:    []int64{300},
		},
		"full deletions of every source in one record": {
			records: [][]byte{
				series(1, false), samples(1, 100), series(2, true), samples(2, 200),
				enc.Tombstones([]tombstones.Stone{
					{Ref: 1, Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: math.MaxInt64}}},
					{Ref: 2, Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: math.MaxInt64}}},
				}, nil),
				series(3, false), samples(3, 300),
			},
			want: []int64{300},
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32768, compression.None)
			require.NoError(t, err)
			for _, r := range tc.records {
				require.NoError(t, w.Log(r))
				_, err := w.NextSegment()
				require.NoError(t, err, "generation state must survive segment boundaries")
			}
			require.NoError(t, w.Close())
			for range 2 {
				h := newFastStartupTestHead(t, dir, false, nil)
				require.NoError(t, h.Init(0))
				got := fastStartupSamples(t, h, "m")
				require.Len(t, got, len(tc.want))
				for i, ts := range tc.want {
					require.Equal(t, ts, got[i].T())
				}
				require.NoError(t, h.Close())
			}
		})
	}
}

func TestHeadFastStartupMixedSamples(t *testing.T) {
	for _, kind := range []string{"exponential", "custom buckets", "gauge"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			lset := labels.FromStrings("__name__", "m")
			hists := tsdbutil.GenerateTestHistograms(20)
			fhists := tsdbutil.GenerateTestFloatHistograms(20)
			switch kind {
			case "custom buckets":
				hists = tsdbutil.GenerateTestCustomBucketsHistograms(20)
				fhists = tsdbutil.GenerateTestCustomBucketsFloatHistograms(20)
			case "gauge":
				hists = tsdbutil.GenerateTestGaugeHistograms(20)
				fhists = tsdbutil.GenerateTestGaugeFloatHistograms(20)
			}
			appendMixed := func(h *Head, offset int64) {
				h.opts.EnableSTStorage.Store(true)
				h.opts.FloatChunkEncoding.Store(uint32(chunkenc.EncXOR2))
				a := h.AppenderV2(context.Background())
				for i := range 20 {
					ts := 1000 + int64(i)*10 + offset
					var err error
					switch i % 3 {
					case 0:
						_, err = a.Append(0, lset, 100, ts, float64(i), nil, nil, storage.AOptions{})
					case 1:
						_, err = a.Append(0, lset, 0, ts, 0, hists[i], nil, storage.AOptions{})
					case 2:
						_, err = a.Append(0, lset, 0, ts, 0, nil, fhists[i], storage.AOptions{})
					}
					require.NoError(t, err)
				}
				require.NoError(t, a.Commit())
			}
			h1 := newFastStartupTestHead(t, dir, false, nil)
			require.NoError(t, h1.Init(0))
			appendMixed(h1, 0)
			require.NoError(t, h1.Close())
			h2, resume := restartFastStartupPaused(t, dir)
			appendMixed(h2, 5)
			resume()
			require.NoError(t, h2.WALReplayError())
			check := func(h *Head) {
				got := fastStartupSamples(t, h, "m")
				require.Len(t, got, 40)
				for i, s := range got {
					require.Equal(t, 1000+int64(i)*5, s.T())
					switch (i / 2) % 3 {
					case 0:
						require.Equal(t, chunkenc.ValFloat, s.Type())
						require.Equal(t, int64(100), s.ST())
						require.Equal(t, float64(i/2), s.F())
					case 1:
						want := hists[i/2].Copy()
						got := s.H().Copy()
						if want.CounterResetHint != histogram.GaugeType {
							want.CounterResetHint = histogram.UnknownCounterReset
							got.CounterResetHint = histogram.UnknownCounterReset
						}
						require.Equal(t, want, got)
					case 2:
						want := fhists[i/2].Copy()
						got := s.FH().Copy()
						if want.CounterResetHint != histogram.GaugeType {
							want.CounterResetHint = histogram.UnknownCounterReset
							got.CounterResetHint = histogram.UnknownCounterReset
						}
						require.Equal(t, want, got)
					}
				}
			}
			check(h2)
			require.NoError(t, h2.Close())
			for range 2 {
				h := newFastStartupTestHead(t, dir, false, nil)
				h.opts.EnableSTStorage.Store(true)
				h.opts.FloatChunkEncoding.Store(uint32(chunkenc.EncXOR2))
				require.NoError(t, h.Init(0))
				check(h)
				require.NoError(t, h.Close())
			}
		})
	}
}

func TestHeadConcurrentWALSnapshotFallback(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 300, 400)
	resume()
	require.NoError(t, h2.WALReplayError())
	require.NoError(t, h2.Close())
	h3 := newFastStartupTestHead(t, dir, false, nil)
	h3.opts.EnableMemorySnapshotOnShutdown = true
	require.NoError(t, h3.Init(0))
	require.NoError(t, h3.Close())
	_, _, _, err := LastChunkSnapshot(dir)
	require.ErrorIs(t, err, record.ErrNotFound)
	h4 := newFastStartupTestHead(t, dir, false, nil)
	h4.opts.EnableMemorySnapshotOnShutdown = true
	require.NoError(t, h4.Init(0))
	require.Len(t, fastStartupSamples(t, h4, "m"), 4)
}

func TestHeadWALRecreatedSeriesSnapshot(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	enc := record.Encoder{}
	w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32768, compression.None)
	require.NoError(t, err)
	var samples []record.RefSample
	for ts := int64(100); ts < 500; ts++ {
		samples = append(samples, record.RefSample{Ref: 2, T: ts, V: float64(ts)})
	}
	require.NoError(t, w.Log(
		enc.Series([]record.RefSeries{{Ref: 1, Labels: lset}, {Ref: 2, Labels: lset}}, nil),
		enc.Samples(samples, nil),
	))
	require.NoError(t, w.Close())
	for range 3 {
		h := newFastStartupTestHead(t, dir, false, nil)
		h.opts.EnableMemorySnapshotOnShutdown = true
		require.NoError(t, h.Init(0))
		require.Len(t, fastStartupSamples(t, h, "m"), 400)
		require.NoError(t, h.Close())
	}
}

func TestHeadFastStartupCancelWhileDraining(t *testing.T) {
	dir := t.TempDir()
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, labels.FromStrings("__name__", "old"), 100)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	a := h2.Appender(context.Background())
	_, err := a.Append(0, labels.FromStrings("__name__", "live"), 200, 2)
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { resume(); close(done) }()
	require.Eventually(t, func() bool {
		h2.replayAppendersMtx.Lock()
		defer h2.replayAppendersMtx.Unlock()
		return h2.replayMerging
	}, time.Second, time.Millisecond)
	// Cancel without closing the WAL so the outstanding appender can roll back.
	h2.walReplayCancel()
	<-done
	require.ErrorIs(t, h2.WALReplayError(), context.Canceled)
	require.NoError(t, a.Rollback())
	require.NoError(t, h2.Close())
}

func TestHeadConcurrentWALRepairTail(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 300, 400)
	resume()
	require.NoError(t, h2.WALReplayError())
	require.NoError(t, h2.Close())
	w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32768, compression.None)
	require.NoError(t, err)
	// Valid WAL framing but invalid sample payload, after all valid data.
	require.NoError(t, w.Log([]byte{byte(record.Samples), 1}))
	require.NoError(t, w.Close())
	db, err := Open(dir, nil, nil, DefaultOptions(), nil)
	require.NoError(t, err)
	defer db.Close()
	require.Len(t, fastStartupSamples(t, db.Head(), "m"), 4)
}

func TestHeadConcurrentWALOutOfOrderRecovery(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h1 := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h1.Init(0))
	fastStartupAppend(t, h1, lset, 100, 200)
	require.NoError(t, h1.Close())
	h2, resume := restartFastStartupPaused(t, dir)
	fastStartupAppend(t, h2, lset, 300, 400)
	resume()
	require.NoError(t, h2.WALReplayError())
	enableOOO := func(h *Head) {
		var err error
		h.wbl, err = wlog.NewSize(nil, nil, filepath.Join(dir, wlog.WblDirName), 32768, compression.None)
		require.NoError(t, err)
		require.NoError(t, h.SetOutOfOrderTimeWindow(1000, h.wbl))
		h.opts.OutOfOrderCapMax.Store(1)
	}
	enableOOO(h2)
	fastStartupAppend(t, h2, lset, 150, 250, 350)
	check := func(h *Head) {
		q, err := NewBlockQuerier(h, 0, 1000)
		require.NoError(t, err)
		ooo := NewHeadAndOOOQuerier(0, 0, 1000, h, h.oooIso.TrackReadAfter(0), q)
		got := query(t, ooo, labels.MustNewMatcher(labels.MatchEqual, "__name__", "m"))
		require.Len(t, got[lset.String()], 7)
	}
	check(h2)
	require.NoError(t, h2.Close())
	for range 2 {
		h := newFastStartupTestHead(t, dir, false, nil)
		enableOOO(h)
		require.NoError(t, h.Init(0))
		check(h)
		require.NoError(t, h.Close())
	}
	// A later full deletion must also remove chunks loaded through OOO aliases.
	h := newFastStartupTestHead(t, dir, false, nil)
	enableOOO(h)
	require.NoError(t, h.Init(0))
	ref := h.series.getByHash(lset.Hash(), lset).ref
	enc := record.Encoder{}
	require.NoError(t, h.wal.Log(enc.Tombstones([]tombstones.Stone{{
		Ref: storage.SeriesRef(ref), Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: math.MaxInt64}},
	}}, nil)))
	require.NoError(t, h.Close())
	h = newFastStartupTestHead(t, dir, false, nil)
	enableOOO(h)
	require.NoError(t, h.Init(0))
	require.Zero(t, h.NumSeries())
	require.Zero(t, prom_testutil.ToFloat64(h.metrics.chunks))
}

func TestHeadConcurrentWALInterruptedReplay(t *testing.T) {
	for _, corruptHistory := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "historical corruption"}[corruptHistory], func(t *testing.T) {
			dir := t.TempDir()
			lset := labels.FromStrings("__name__", "m")
			h1 := newFastStartupTestHead(t, dir, false, nil)
			require.NoError(t, h1.Init(0))
			fastStartupAppend(t, h1, lset, 100, 200)
			if corruptHistory {
				require.NoError(t, h1.wal.Log([]byte{byte(record.Samples), 1}))
			}
			require.NoError(t, h1.Close())
			h2, resume := restartFastStartupPaused(t, dir)
			fastStartupAppend(t, h2, lset, 150, 300)
			liveSegment, _, err := h2.wal.LastSegmentAndOffset()
			require.NoError(t, err)
			if !corruptHistory {
				h2.walReplayCancel()
			}
			resume()
			require.Error(t, h2.WALReplayError())
			require.NoError(t, h2.Close())
			if corruptHistory {
				path := wlog.SegmentName(filepath.Join(dir, "wal"), liveSegment)
				before, err := os.ReadFile(path)
				require.NoError(t, err)
				db, err := Open(dir, nil, nil, DefaultOptions(), nil)
				require.ErrorContains(t, err, "refusing WAL repair")
				require.Nil(t, db)
				after, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, before, after)
				return
			}
			h3 := newFastStartupTestHead(t, dir, false, nil)
			require.NoError(t, h3.Init(0))
			require.Len(t, fastStartupSamples(t, h3, "m"), 4)
		})
	}
}

func TestHeadFastStartupProcessCrash(t *testing.T) {
	const dirEnv = "PROMETHEUS_TEST_FAST_STARTUP_CRASH_DIR"
	const phaseEnv = "PROMETHEUS_TEST_FAST_STARTUP_CRASH_PHASE"
	lset := labels.FromStrings("__name__", "m")
	if dir := os.Getenv(dirEnv); dir != "" {
		h, resume := restartFastStartupPaused(t, dir)
		fastStartupAppend(t, h, lset, rangeSlice(300, 700)...)
		// Exercise both source-local caches and chunks reconciled by a merge.
		h.mmapHeadChunks()
		if os.Getenv(phaseEnv) == "after merge" {
			resume()
			require.NoError(t, h.WALReplayError())
			fastStartupAppend(t, h, lset, 700)
			h.mmapHeadChunks()
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "ready"), nil, 0o600))
		// The parent kills this process without running Head.Close or cleanups.
		select {}
	}

	for _, phase := range []string{"during replay", "after merge"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			h := newFastStartupTestHead(t, dir, false, nil)
			require.NoError(t, h.Init(0))
			fastStartupAppend(t, h, lset, rangeSlice(100, 500)...)
			require.NoError(t, h.Close())
			executable, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestHeadFastStartupProcessCrash$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), dirEnv+"="+dir, phaseEnv+"="+phase)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			require.NoError(t, cmd.Start())
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
				if t.Failed() {
					t.Log(output.String())
				}
			})
			require.Eventually(t, func() bool {
				_, err := os.Stat(filepath.Join(dir, "ready"))
				return err == nil
			}, 15*time.Second, 10*time.Millisecond)
			require.NoError(t, cmd.Process.Kill())
			require.Error(t, cmd.Wait())
			for range 2 {
				h = newFastStartupTestHead(t, dir, false, nil)
				require.NoError(t, h.Init(0))
				got := fastStartupSamples(t, h, "m")
				want := 600
				if phase == "after merge" {
					want++
				}
				require.Len(t, got, want)
				for i, s := range got {
					require.Equal(t, int64(100+i), s.T())
					require.Equal(t, float64(100+i), s.F())
				}
				require.NoError(t, h.Close())
			}
		})
	}
}

func TestHeadConcurrentWALMetadataCheckpointOrder(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	enc := record.Encoder{}
	w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32768, compression.None)
	require.NoError(t, err)
	require.NoError(t, w.Log(
		enc.Series([]record.RefSeries{{Ref: 1, Labels: lset}}, nil),
		enc.Samples([]record.RefSample{{Ref: 1, T: 100, V: 1}}, nil),
		enc.ConcurrentSeries([]record.RefSeries{{Ref: 2, Labels: lset}}, nil),
		enc.Samples([]record.RefSample{{Ref: 2, T: 200, V: 2}}, nil),
		enc.Metadata([]record.RefMetadata{{Ref: 2, Help: "older update"}, {Ref: 1, Help: "latest update"}}, nil),
	))
	next, err := w.NextSegment()
	require.NoError(t, err)
	_, err = wlog.Checkpoint(slog.New(slog.DiscardHandler), w, 0, next-1, func(chunks.HeadSeriesRef) bool { return true }, 0, false, true)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	h := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h.Init(0))
	s := h.series.getByHash(lset.Hash(), lset)
	require.NotNil(t, s.meta)
	require.Equal(t, "latest update", s.meta.Help)
}

func TestHeadConcurrentWALMetadataAliasExpiry(t *testing.T) {
	for _, mode := range []string{"ordinary", "repair", "fast moved", "fast merged"} {
		for _, metadataOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/metadata_only=%t", mode, metadataOnly), func(t *testing.T) {
				dir := t.TempDir()
				lset := labels.FromStrings("__name__", "m")
				enc := record.Encoder{}
				w, err := wlog.NewSize(nil, nil, filepath.Join(dir, "wal"), 32768, compression.None)
				require.NoError(t, err)
				// The newer source survives. The older one carries the latest metadata update.
				require.NoError(t, w.Log(enc.Series([]record.RefSeries{{Ref: 1, Labels: lset}}, nil)))
				if !metadataOnly {
					require.NoError(t, w.Log(enc.Samples([]record.RefSample{{Ref: 1, T: 100, V: 1}}, nil)))
				}
				require.NoError(t, w.Log(
					enc.ConcurrentSeries([]record.RefSeries{{Ref: 2, Labels: lset}}, nil),
					enc.Samples([]record.RefSample{{Ref: 2, T: 200, V: 2}}, nil),
					enc.Metadata([]record.RefMetadata{{Ref: 2, Help: "old metadata"}}, nil),
					enc.Metadata([]record.RefMetadata{{Ref: 1, Help: "latest metadata", Unit: "seconds", Type: uint8(record.Gauge)}}, nil),
				))
				if mode == "repair" {
					require.NoError(t, w.Log([]byte{byte(record.Samples), 1}))
				}
				require.NoError(t, w.Close())

				var h *Head
				var closeHead func() error
				switch mode {
				case "ordinary":
					h = newFastStartupTestHead(t, dir, false, nil)
					require.NoError(t, h.Init(0))
					closeHead = h.Close
				case "repair":
					db, err := Open(dir, nil, nil, DefaultOptions(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = db.Close() })
					h, closeHead = db.Head(), db.Close
				default:
					var resume func()
					h, resume = restartFastStartupPaused(t, dir)
					if mode == "fast merged" {
						fastStartupAppend(t, h, lset, 300)
					}
					resume()
					require.NoError(t, h.WALReplayError())
					closeHead = h.Close
				}
				check := func(h *Head) {
					s := h.series.getByHash(lset.Hash(), lset)
					require.NotNil(t, s)
					require.NotNil(t, s.meta)
					require.Equal(t, "latest metadata", s.meta.Help)
					require.Equal(t, "seconds", s.meta.Unit)
					require.Equal(t, record.ToMetricType(uint8(record.Gauge)), s.meta.Type)
				}
				check(h)
				// Expire the metadata's original source while retaining the series.
				// Neither its definition nor its metadata will survive checkpointing.
				keep := h.keepSeriesInWALCheckpointFn(150)
				require.False(t, keep(1))
				next, err := h.wal.NextSegment()
				require.NoError(t, err)
				_, err = wlog.Checkpoint(h.logger, h.wal, 0, next-1, keep, 150, false, true)
				require.NoError(t, err)
				require.NoError(t, h.wal.Truncate(next))
				require.NoError(t, closeHead())
				for range 2 {
					h = newFastStartupTestHead(t, dir, false, nil)
					require.NoError(t, h.Init(150))
					check(h)
					require.NoError(t, h.Close())
				}
			})
		}
	}
}

func TestHeadFastStartupRepeatedGenerations(t *testing.T) {
	dir := t.TempDir()
	lset := labels.FromStrings("__name__", "m")
	h := newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h.Init(0))
	fastStartupAppend(t, h, lset, 100, 200)
	require.NoError(t, h.Close())
	for i := range 4 {
		h, resume := restartFastStartupPaused(t, dir)
		fastStartupAppend(t, h, lset, int64(300+i*100))
		resume()
		require.NoError(t, h.WALReplayError())
		require.Len(t, fastStartupSamples(t, h, "m"), 3+i)
		require.NoError(t, h.Close())
	}
	h = newFastStartupTestHead(t, dir, false, nil)
	require.NoError(t, h.Init(0))
	require.Len(t, fastStartupSamples(t, h, "m"), 6)
}

func TestHeadStaleCountAcrossSampleTypes(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot=%t", snapshot), func(t *testing.T) {
			dir := t.TempDir()
			newHead := func() *Head {
				h := newFastStartupTestHead(t, dir, false, nil)
				h.opts.EnableMemorySnapshotOnShutdown = snapshot
				require.NoError(t, h.Init(0))
				return h
			}
			lset := labels.FromStrings("__name__", "m")
			stale := math.Float64frombits(value.StaleNaN)
			for i, tc := range []struct {
				v     float64
				h     *histogram.Histogram
				fh    *histogram.FloatHistogram
				stale uint64
			}{
				{v: stale, stale: 1},
				{h: tsdbutil.GenerateTestHistogram(1)},
				{h: &histogram.Histogram{Sum: stale}, stale: 1},
				{fh: tsdbutil.GenerateTestFloatHistogram(1)},
				{fh: &histogram.FloatHistogram{Sum: stale}, stale: 1},
				{v: 1},
			} {
				h := newHead()
				a := h.AppenderV2(context.Background())
				_, err := a.Append(0, lset, 0, int64(i+1)*100, tc.v, tc.h, tc.fh, storage.AOptions{})
				require.NoError(t, err)
				require.NoError(t, a.Commit())
				require.Equal(t, tc.stale, h.NumStaleSeries())
				require.NoError(t, h.Close())
				if snapshot {
					_, _, _, err := LastChunkSnapshot(dir)
					require.NoError(t, err)
				}
				h = newHead()
				require.Equal(t, tc.stale, h.NumStaleSeries())
				require.NoError(t, h.Close())
			}
		})
	}
}
