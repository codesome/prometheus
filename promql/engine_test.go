// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestQueryConcurrency(t *testing.T) {
	maxConcurrency := 10

	dir, err := ioutil.TempDir("", "test_concurrency")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	queryTracker := NewActiveQueryTracker(dir, maxConcurrency, nil)

	opts := EngineOpts{
		Logger:             nil,
		Reg:                nil,
		MaxSamples:         10,
		Timeout:            100 * time.Second,
		ActiveQueryTracker: queryTracker,
	}

	engine := NewEngine(opts)
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	block := make(chan struct{})
	processing := make(chan struct{})

	f := func(context.Context) error {
		processing <- struct{}{}
		<-block
		return nil
	}

	for i := 0; i < maxConcurrency; i++ {
		q := engine.newTestQuery(f)
		go q.Exec(ctx)
		select {
		case <-processing:
			// Expected.
		case <-time.After(20 * time.Millisecond):
			t.Fatalf("Query within concurrency threshold not being executed")
		}
	}

	q := engine.newTestQuery(f)
	go q.Exec(ctx)

	select {
	case <-processing:
		t.Fatalf("Query above concurrency threshold being executed")
	case <-time.After(20 * time.Millisecond):
		// Expected.
	}

	// Terminate a running query.
	block <- struct{}{}

	select {
	case <-processing:
		// Expected.
	case <-time.After(20 * time.Millisecond):
		t.Fatalf("Query within concurrency threshold not being executed")
	}

	// Terminate remaining queries.
	for i := 0; i < maxConcurrency; i++ {
		block <- struct{}{}
	}
}

func TestQueryTimeout(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    5 * time.Millisecond,
	}
	engine := NewEngine(opts)
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	query := engine.newTestQuery(func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		return contextDone(ctx, "test statement execution")
	})

	res := query.Exec(ctx)
	require.Error(t, res.Err, "expected timeout error but got none")

	var e ErrQueryTimeout
	require.True(t, errors.As(res.Err, &e), "expected timeout error but got: %s", res.Err)
}

const errQueryCanceled = ErrQueryCanceled("test statement execution")

func TestQueryCancel(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    10 * time.Second,
	}
	engine := NewEngine(opts)
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	// Cancel a running query before it completes.
	block := make(chan struct{})
	processing := make(chan struct{})

	query1 := engine.newTestQuery(func(ctx context.Context) error {
		processing <- struct{}{}
		<-block
		return contextDone(ctx, "test statement execution")
	})

	var res *Result

	go func() {
		res = query1.Exec(ctx)
		processing <- struct{}{}
	}()

	<-processing
	query1.Cancel()
	block <- struct{}{}
	<-processing

	require.Error(t, res.Err, "expected cancellation error for query1 but got none")
	require.Equal(t, errQueryCanceled, res.Err)

	// Canceling a query before starting it must have no effect.
	query2 := engine.newTestQuery(func(ctx context.Context) error {
		return contextDone(ctx, "test statement execution")
	})

	query2.Cancel()
	res = query2.Exec(ctx)
	require.NoError(t, res.Err)
}

// errQuerier implements storage.Querier which always returns error.
type errQuerier struct {
	err error
}

func (q *errQuerier) Select(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
	return errSeriesSet{err: q.err}
}
func (*errQuerier) LabelValues(string) ([]string, storage.Warnings, error) { return nil, nil, nil }
func (*errQuerier) LabelNames() ([]string, storage.Warnings, error)        { return nil, nil, nil }
func (*errQuerier) Close() error                                           { return nil }

// errSeriesSet implements storage.SeriesSet which always returns error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool                   { return false }
func (errSeriesSet) At() storage.Series           { return nil }
func (e errSeriesSet) Err() error                 { return e.err }
func (e errSeriesSet) Warnings() storage.Warnings { return nil }

func TestQueryError(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    10 * time.Second,
	}
	engine := NewEngine(opts)
	errStorage := ErrStorage{errors.New("storage error")}
	queryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &errQuerier{err: errStorage}, nil
	})
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	vectorQuery, err := engine.NewInstantQuery(queryable, "foo", time.Unix(1, 0))
	require.NoError(t, err)

	res := vectorQuery.Exec(ctx)
	require.Error(t, res.Err, "expected error on failed select but got none")
	require.True(t, errors.Is(res.Err, errStorage), "expected error doesn't match")

	matrixQuery, err := engine.NewInstantQuery(queryable, "foo[1m]", time.Unix(1, 0))
	require.NoError(t, err)

	res = matrixQuery.Exec(ctx)
	require.Error(t, res.Err, "expected error on failed select but got none")
	require.True(t, errors.Is(res.Err, errStorage), "expected error doesn't match")
}

type noopHintRecordingQueryable struct {
	hints []*storage.SelectHints
}

func (h *noopHintRecordingQueryable) Querier(context.Context, int64, int64) (storage.Querier, error) {
	return &hintRecordingQuerier{Querier: &errQuerier{}, h: h}, nil
}

type hintRecordingQuerier struct {
	storage.Querier

	h *noopHintRecordingQueryable
}

func (h *hintRecordingQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	h.h.hints = append(h.h.hints, hints)
	return h.Querier.Select(sortSeries, hints, matchers...)
}

func TestSelectHintsSetCorrectly(t *testing.T) {
	opts := EngineOpts{
		Logger:        nil,
		Reg:           nil,
		MaxSamples:    10,
		Timeout:       10 * time.Second,
		LookbackDelta: 5 * time.Second,
	}

	for _, tc := range []struct {
		query string

		// All times are in milliseconds.
		start int64
		end   int64

		// TODO(bwplotka): Add support for better hints when subquerying.
		expected []*storage.SelectHints
	}{{
		query: "foo", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 5000, End: 10000},
		},
	}, {
		query: "foo @ 15", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 10000, End: 15000},
		},
	}, {
		query: "foo @ 1", start: 10000,
		expected: []*storage.SelectHints{
			{Start: -4000, End: 1000},
		},
	}, {
		query: "foo[2m]", start: 200000,
		expected: []*storage.SelectHints{
			{Start: 80000, End: 200000, Range: 120000},
		},
	}, {
		query: "foo[2m] @ 180", start: 200000,
		expected: []*storage.SelectHints{
			{Start: 60000, End: 180000, Range: 120000},
		},
	}, {
		query: "foo[2m] @ 300", start: 200000,
		expected: []*storage.SelectHints{
			{Start: 180000, End: 300000, Range: 120000},
		},
	}, {
		query: "foo[2m] @ 60", start: 200000,
		expected: []*storage.SelectHints{
			{Start: -60000, End: 60000, Range: 120000},
		},
	}, {
		query: "foo[2m] offset 2m", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 60000, End: 180000, Range: 120000},
		},
	}, {
		query: "foo[2m] @ 200 offset 2m", start: 300000,
		expected: []*storage.SelectHints{
			{Start: -40000, End: 80000, Range: 120000},
		},
	}, {
		query: "foo[2m:1s]", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 175000, End: 300000},
		},
	}, {
		query: "count_over_time(foo[2m:1s])", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 175000, End: 300000, Func: "count_over_time"},
		},
	}, {
		query: "count_over_time(foo[2m:1s] @ 300)", start: 200000,
		expected: []*storage.SelectHints{
			{Start: 175000, End: 300000, Func: "count_over_time"},
		},
	}, {
		query: "count_over_time(foo[2m:1s] @ 200)", start: 200000,
		expected: []*storage.SelectHints{
			{Start: 75000, End: 200000, Func: "count_over_time"},
		},
	}, {
		query: "count_over_time(foo[2m:1s] @ 100)", start: 200000,
		expected: []*storage.SelectHints{
			{Start: -25000, End: 100000, Func: "count_over_time"},
		},
	}, {
		query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 165000, End: 290000, Func: "count_over_time"},
		},
	}, {
		query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 155000, End: 280000, Func: "count_over_time"},
		},
	}, {
		// When the @ is on the vector selector, the enclosing subquery parameters
		// don't affect the hint ranges.
		query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 185000, End: 190000, Func: "count_over_time"},
		},
	}, {
		// When the @ is on the vector selector, the enclosing subquery parameters
		// don't affect the hint ranges.
		query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000,
		expected: []*storage.SelectHints{
			{Start: 185000, End: 190000, Func: "count_over_time"},
		},
	}, {
		query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000,
		expected: []*storage.SelectHints{
			{Start: -45000, End: 80000, Func: "count_over_time"},
		},
	}, {

		query: "foo", start: 10000, end: 20000,
		expected: []*storage.SelectHints{
			{Start: 5000, End: 20000, Step: 1000},
		},
	}, {
		query: "foo @ 15", start: 10000, end: 20000,
		expected: []*storage.SelectHints{
			{Start: 10000, End: 15000, Step: 1000},
		},
	}, {
		query: "foo @ 1", start: 10000, end: 20000,
		expected: []*storage.SelectHints{
			{Start: -4000, End: 1000, Step: 1000},
		},
	}, {
		query: "rate(foo[2m] @ 180)", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 60000, End: 180000, Range: 120000, Func: "rate", Step: 1000},
		},
	}, {
		query: "rate(foo[2m] @ 300)", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 180000, End: 300000, Range: 120000, Func: "rate", Step: 1000},
		},
	}, {
		query: "rate(foo[2m] @ 60)", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: -60000, End: 60000, Range: 120000, Func: "rate", Step: 1000},
		},
	}, {
		query: "rate(foo[2m])", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 80000, End: 500000, Range: 120000, Func: "rate", Step: 1000},
		},
	}, {
		query: "rate(foo[2m] offset 2m)", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 60000, End: 380000, Range: 120000, Func: "rate", Step: 1000},
		},
	}, {
		query: "rate(foo[2m:1s])", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 175000, End: 500000, Func: "rate", Step: 1000},
		},
	}, {
		query: "count_over_time(foo[2m:1s])", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 175000, End: 500000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 165000, End: 490000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "count_over_time(foo[2m:1s] @ 300)", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 175000, End: 300000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "count_over_time(foo[2m:1s] @ 200)", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 75000, End: 200000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "count_over_time(foo[2m:1s] @ 100)", start: 200000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: -25000, End: 100000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 155000, End: 480000, Func: "count_over_time", Step: 1000},
		},
	}, {
		// When the @ is on the vector selector, the enclosing subquery parameters
		// don't affect the hint ranges.
		query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 185000, End: 190000, Func: "count_over_time", Step: 1000},
		},
	}, {
		// When the @ is on the vector selector, the enclosing subquery parameters
		// don't affect the hint ranges.
		query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 185000, End: 190000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: -45000, End: 80000, Func: "count_over_time", Step: 1000},
		},
	}, {
		query: "sum by (dim1) (foo)", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 5000, End: 10000, Func: "sum", By: true, Grouping: []string{"dim1"}},
		},
	}, {
		query: "sum without (dim1) (foo)", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 5000, End: 10000, Func: "sum", Grouping: []string{"dim1"}},
		},
	}, {
		query: "sum by (dim1) (avg_over_time(foo[1s]))", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 9000, End: 10000, Func: "avg_over_time", Range: 1000},
		},
	}, {
		query: "sum by (dim1) (max by (dim2) (foo))", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 5000, End: 10000, Func: "max", By: true, Grouping: []string{"dim2"}},
		},
	}, {
		query: "(max by (dim1) (foo))[5s:1s]", start: 10000,
		expected: []*storage.SelectHints{
			{Start: 0, End: 10000, Func: "max", By: true, Grouping: []string{"dim1"}},
		},
	}, {
		query: "(sum(http_requests{group=~\"p.*\"})+max(http_requests{group=~\"c.*\"}))[20s:5s]", start: 120000,
		expected: []*storage.SelectHints{
			{Start: 95000, End: 120000, Func: "sum", By: true},
			{Start: 95000, End: 120000, Func: "max", By: true},
		},
	}, {
		query: "foo @ 50 + bar @ 250 + baz @ 900", start: 100000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 45000, End: 50000, Step: 1000},
			{Start: 245000, End: 250000, Step: 1000},
			{Start: 895000, End: 900000, Step: 1000},
		},
	}, {
		query: "foo @ 50 + bar + baz @ 900", start: 100000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 45000, End: 50000, Step: 1000},
			{Start: 95000, End: 500000, Step: 1000},
			{Start: 895000, End: 900000, Step: 1000},
		},
	}, {
		query: "rate(foo[2s] @ 50) + bar @ 250 + baz @ 900", start: 100000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 48000, End: 50000, Step: 1000, Func: "rate", Range: 2000},
			{Start: 245000, End: 250000, Step: 1000},
			{Start: 895000, End: 900000, Step: 1000},
		},
	}, {
		query: "rate(foo[2s:1s] @ 50) + bar + baz", start: 100000, end: 500000,
		expected: []*storage.SelectHints{
			{Start: 43000, End: 50000, Step: 1000, Func: "rate"},
			{Start: 95000, End: 500000, Step: 1000},
			{Start: 95000, End: 500000, Step: 1000},
		},
	},
		{
			query: "rate(foo[2s:1s] @ 50) + bar + rate(baz[2m:1s] @ 900 offset 2m) ", start: 100000, end: 500000,
			expected: []*storage.SelectHints{
				{Start: 43000, End: 50000, Step: 1000, Func: "rate"},
				{Start: 95000, End: 500000, Step: 1000},
				{Start: 655000, End: 780000, Step: 1000, Func: "rate"},
			},
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			engine := NewEngine(opts)
			hintsRecorder := &noopHintRecordingQueryable{}

			var (
				query Query
				err   error
			)
			if tc.end == 0 {
				query, err = engine.NewInstantQuery(hintsRecorder, tc.query, timestamp.Time(tc.start))
			} else {
				query, err = engine.NewRangeQuery(hintsRecorder, tc.query, timestamp.Time(tc.start), timestamp.Time(tc.end), time.Second)
			}
			require.NoError(t, err)

			res := query.Exec(context.Background())
			require.NoError(t, res.Err)

			require.Equal(t, tc.expected, hintsRecorder.hints)
		})

	}
}

func TestFindMintMaxt(t *testing.T) {
	opts := EngineOpts{
		MaxSamples:    10,
		Timeout:       10 * time.Second,
		LookbackDelta: 5 * time.Second,
	}

	for _, tc := range []struct {
		query string

		// All times are in milliseconds.
		start, end       int64
		expMint, expMaxt int64
	}{
		{query: "foo", start: 10000, expMint: 5000, expMaxt: 10000},
		{query: "foo @ 15", start: 10000, expMint: 10000, expMaxt: 15000},
		{query: "foo @ 1", start: 10000, expMint: -4000, expMaxt: 1000},
		{query: "foo[2m]", start: 200000, expMint: 80000, expMaxt: 200000},
		{query: "foo[2m] @ 180", start: 200000, expMint: 60000, expMaxt: 180000},
		{query: "foo[2m] @ 300", start: 200000, expMint: 180000, expMaxt: 300000},
		{query: "foo[2m] @ 60", start: 200000, expMint: -60000, expMaxt: 60000},
		{query: "foo[2m] offset 2m", start: 300000, expMint: 60000, expMaxt: 300000},
		{query: "foo[2m] @ 200 offset 2m", start: 300000, expMint: -40000, expMaxt: 80000},
		{query: "foo[2m:1s]", start: 300000, expMint: 175000, expMaxt: 300000},
		{query: "count_over_time(foo[2m:1s])", start: 300000, expMint: 175000, expMaxt: 300000},
		{query: "count_over_time(foo[2m:1s] @ 300)", start: 200000, expMint: 175000, expMaxt: 300000},
		{query: "count_over_time(foo[2m:1s] @ 200)", start: 200000, expMint: 75000, expMaxt: 200000},
		{query: "count_over_time(foo[2m:1s] @ 100)", start: 200000, expMint: -25000, expMaxt: 100000},
		{query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000, expMint: 165000, expMaxt: 300000},
		{query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000, expMint: 155000, expMaxt: 300000},
		{
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the mint.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000, expMint: 185000, expMaxt: 190000,
		},
		{
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the mint.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, expMint: 185000, expMaxt: 190000,
		},
		{query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, expMint: -45000, expMaxt: 80000},
		{query: "foo", start: 10000, end: 20000, expMint: 5000, expMaxt: 20000},
		{query: "foo @ 15", start: 10000, end: 20000, expMint: 10000, expMaxt: 15000},
		{query: "foo @ 1", start: 10000, end: 20000, expMint: -4000, expMaxt: 1000},
		{query: "rate(foo[2m] @ 180)", start: 200000, end: 500000, expMint: 60000, expMaxt: 180000},
		{query: "rate(foo[2m] @ 300)", start: 200000, end: 500000, expMint: 180000, expMaxt: 300000},
		{query: "rate(foo[2m] @ 60)", start: 200000, end: 500000, expMint: -60000, expMaxt: 60000},
		{query: "rate(foo[2m])", start: 200000, end: 500000, expMint: 80000, expMaxt: 500000},
		{query: "rate(foo[2m] offset 2m)", start: 300000, end: 500000, expMint: 60000, expMaxt: 500000},
		{query: "rate(foo[2m:1s])", start: 300000, end: 500000, expMint: 175000, expMaxt: 500000},
		{query: "count_over_time(foo[2m:1s])", start: 300000, end: 500000, expMint: 175000, expMaxt: 500000},
		{query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000, end: 500000, expMint: 165000, expMaxt: 500000},
		{query: "count_over_time(foo[2m:1s] @ 300)", start: 200000, end: 500000, expMint: 175000, expMaxt: 300000},
		{query: "count_over_time(foo[2m:1s] @ 200)", start: 200000, end: 500000, expMint: 75000, expMaxt: 200000},
		{query: "count_over_time(foo[2m:1s] @ 100)", start: 200000, end: 500000, expMint: -25000, expMaxt: 100000},
		{query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000, expMint: 155000, expMaxt: 500000},
		{
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the mint.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000, expMint: 185000, expMaxt: 190000,
		},
		{
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the mint.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000, expMint: 185000, expMaxt: 190000,
		},
		{query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000, expMint: -45000, expMaxt: 80000},
		{query: "sum by (dim1) (foo)", start: 10000, expMint: 5000, expMaxt: 10000},
		{query: "sum without (dim1) (foo)", start: 10000, expMint: 5000, expMaxt: 10000},
		{query: "sum by (dim1) (avg_over_time(foo[1s]))", start: 10000, expMint: 9000, expMaxt: 10000},
		{query: "sum by (dim1) (max by (dim2) (foo))", start: 10000, expMint: 5000, expMaxt: 10000},
		{query: "(max by (dim1) (foo))[5s:1s]", start: 10000, expMint: 0, expMaxt: 10000},
		{query: "foo @ 50 + bar @ 250 + baz @ 900", start: 100000, end: 500000, expMint: 45000, expMaxt: 900000},
		{query: "foo + bar @ 250 + baz @ 900", start: 100000, end: 500000, expMint: 95000, expMaxt: 900000},
		{query: "foo @ 50 + bar @ 250 + baz", start: 100000, end: 500000, expMint: 45000, expMaxt: 500000},
		{query: "foo @ 50 + bar + baz @ 900", start: 100000, end: 500000, expMint: 45000, expMaxt: 900000},
		{query: "rate(foo[2s] @ 50) + bar @ 250 + baz @ 900", start: 100000, end: 500000, expMint: 48000, expMaxt: 900000},
		{query: "rate(foo[2s] @ 50) + bar + baz", start: 100000, end: 500000, expMint: 48000, expMaxt: 500000},
		{query: "rate(foo[2s:1s] @ 50) + bar + baz", start: 100000, end: 500000, expMint: 43000, expMaxt: 500000},
		{query: "rate(foo[2s:1s] @ 50) + bar + rate(baz[2m:1s] @ 900 offset 2m) ", start: 100000, end: 500000, expMint: 43000, expMaxt: 780000},
	} {
		t.Run(tc.query, func(t *testing.T) {
			engine := NewEngine(opts)
			var (
				query Query
				err   error
			)
			if tc.end == 0 {
				query, err = engine.NewInstantQuery(nil, tc.query, timestamp.Time(tc.start))
			} else {
				query, err = engine.NewRangeQuery(nil, tc.query, timestamp.Time(tc.start), timestamp.Time(tc.end), time.Second)
			}
			require.NoError(t, err)
			actMint, actMaxt := engine.findMinMaxTime(query.Statement().(*parser.EvalStmt))
			require.Equal(t, tc.expMint, actMint, "mint")
			require.Equal(t, tc.expMaxt, actMaxt, "maxt")
		})

	}
}

func TestEngineShutdown(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    10 * time.Second,
	}
	engine := NewEngine(opts)
	ctx, cancelCtx := context.WithCancel(context.Background())

	block := make(chan struct{})
	processing := make(chan struct{})

	// Shutdown engine on first handler execution. Should handler execution ever become
	// concurrent this test has to be adjusted accordingly.
	f := func(ctx context.Context) error {
		processing <- struct{}{}
		<-block
		return contextDone(ctx, "test statement execution")
	}
	query1 := engine.newTestQuery(f)

	// Stopping the engine must cancel the base context. While executing queries is
	// still possible, their context is canceled from the beginning and execution should
	// terminate immediately.

	var res *Result
	go func() {
		res = query1.Exec(ctx)
		processing <- struct{}{}
	}()

	<-processing
	cancelCtx()
	block <- struct{}{}
	<-processing

	require.Error(t, res.Err, "expected error on shutdown during query but got none")
	require.Equal(t, errQueryCanceled, res.Err)

	query2 := engine.newTestQuery(func(context.Context) error {
		t.Fatalf("reached query execution unexpectedly")
		return nil
	})

	// The second query is started after the engine shut down. It must
	// be canceled immediately.
	res2 := query2.Exec(ctx)
	require.Error(t, res2.Err, "expected error on querying with canceled context but got none")

	var e ErrQueryCanceled
	require.True(t, errors.As(res2.Err, &e), "expected cancellation error but got: %s", res2.Err)
}

func TestEngineEvalStmtTimestamps(t *testing.T) {
	test, err := NewTest(t, `
load 10s
  metric 1 2
`)
	require.NoError(t, err)
	defer test.Close()

	err = test.Run()
	require.NoError(t, err)

	cases := []struct {
		Query       string
		Result      parser.Value
		Start       time.Time
		End         time.Time
		Interval    time.Duration
		ShouldError bool
	}{
		// Instant queries.
		{
			Query:  "1",
			Result: Scalar{V: 1, T: 1000},
			Start:  time.Unix(1, 0),
		},
		{
			Query: "metric",
			Result: Vector{
				Sample{Point: Point{V: 1, T: 1000},
					Metric: labels.FromStrings("__name__", "metric")},
			},
			Start: time.Unix(1, 0),
		},
		{
			Query: "metric[20s]",
			Result: Matrix{Series{
				Points: []Point{{V: 1, T: 0}, {V: 2, T: 10000}},
				Metric: labels.FromStrings("__name__", "metric")},
			},
			Start: time.Unix(10, 0),
		},
		// Range queries.
		{
			Query: "1",
			Result: Matrix{Series{
				Points: []Point{{V: 1, T: 0}, {V: 1, T: 1000}, {V: 1, T: 2000}},
				Metric: labels.FromStrings()},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(2, 0),
			Interval: time.Second,
		},
		{
			Query: "metric",
			Result: Matrix{Series{
				Points: []Point{{V: 1, T: 0}, {V: 1, T: 1000}, {V: 1, T: 2000}},
				Metric: labels.FromStrings("__name__", "metric")},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(2, 0),
			Interval: time.Second,
		},
		{
			Query: "metric",
			Result: Matrix{Series{
				Points: []Point{{V: 1, T: 0}, {V: 1, T: 5000}, {V: 2, T: 10000}},
				Metric: labels.FromStrings("__name__", "metric")},
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(10, 0),
			Interval: 5 * time.Second,
		},
		{
			Query:       `count_values("wrong label!", metric)`,
			ShouldError: true,
		},
	}

	for _, c := range cases {
		var err error
		var qry Query
		if c.Interval == 0 {
			qry, err = test.QueryEngine().NewInstantQuery(test.Queryable(), c.Query, c.Start)
		} else {
			qry, err = test.QueryEngine().NewRangeQuery(test.Queryable(), c.Query, c.Start, c.End, c.Interval)
		}
		require.NoError(t, err)

		res := qry.Exec(test.Context())
		if c.ShouldError {
			require.Error(t, res.Err, "expected error for the query %q", c.Query)
			continue
		}

		require.NoError(t, res.Err)
		require.Equal(t, c.Result, res.Value, "query %q failed", c.Query)
	}
}

func TestAtModifier(t *testing.T) {
	test, err := NewTest(t, `
load 10s
  metric{job="1"} 0+1x1000
  metric{job="2"} 0+2x1000

load 1ms
  metric_ms 0+1x10000
`)
	require.NoError(t, err)
	defer test.Close()

	err = test.Run()
	require.NoError(t, err)

	lbls1 := labels.FromStrings("__name__", "metric", "job", "1")
	lbls2 := labels.FromStrings("__name__", "metric", "job", "2")
	lblsms := labels.FromStrings("__name__", "metric_ms")

	cases := []struct {
		query                string
		start, end, interval int64 // Time in second.
		result               parser.Value
	}{
		{ // Time of the result is the evaluation time.
			query: `metric{job="1"} @ 50`,
			start: 25,
			result: Vector{
				Sample{Point: Point{V: 5, T: 25000}, Metric: lbls1},
			},
		}, { // Time of the result is the evaluation time.
			query: `metric{job="2"} @ 50`,
			start: 100, end: 102,
			result: Matrix{
				Series{
					Points: []Point{{V: 10, T: 100000}, {V: 10, T: 101000}, {V: 10, T: 102000}},
					Metric: lbls2,
				},
			},
		}, { // Timestamps for matrix selector does not depend on the evaluation time.
			query: "metric[20s] @ 300",
			start: 10,
			result: Matrix{
				Series{
					Points: []Point{{V: 28, T: 280000}, {V: 29, T: 290000}, {V: 30, T: 300000}},
					Metric: lbls1,
				},
				Series{
					Points: []Point{{V: 56, T: 280000}, {V: 58, T: 290000}, {V: 60, T: 300000}},
					Metric: lbls2,
				},
			},
		}, { // Timestamps for matrix selector does not depend on the evaluation time.
			query: "metric[20s] @ 300",
			start: 1000,
			result: Matrix{
				Series{
					Points: []Point{{V: 28, T: 280000}, {V: 29, T: 290000}, {V: 30, T: 300000}},
					Metric: lbls1,
				},
				Series{
					Points: []Point{{V: 56, T: 280000}, {V: 58, T: 290000}, {V: 60, T: 300000}},
					Metric: lbls2,
				},
			},
		}, {
			query: `metric_ms @ 1.234`,
			start: 100,
			result: Vector{
				Sample{Point: Point{V: 1234, T: 100000}, Metric: lblsms},
			},
		}, {
			query: `metric_ms[3ms] @ 2.345`,
			start: 100,
			result: Matrix{
				Series{
					Points: []Point{{V: 2342, T: 2342}, {V: 2343, T: 2343}, {V: 2344, T: 2344}, {V: 2345, T: 2345}},
					Metric: lblsms,
				},
			},
		}, {
			query: "metric[100s:25s] @ 300",
			start: 100,
			result: Matrix{
				Series{
					Points: []Point{{V: 20, T: 200000}, {V: 22, T: 225000}, {V: 25, T: 250000}, {V: 27, T: 275000}, {V: 30, T: 300000}},
					Metric: lbls1,
				},
				Series{
					Points: []Point{{V: 40, T: 200000}, {V: 44, T: 225000}, {V: 50, T: 250000}, {V: 54, T: 275000}, {V: 60, T: 300000}},
					Metric: lbls2,
				},
			},
		}, {
			query: "metric[100s:25s] @ 50",
			start: 100,
			result: Matrix{
				Series{
					Points: []Point{{V: 0, T: 0}, {V: 2, T: 25000}, {V: 5, T: 50000}},
					Metric: lbls1,
				},
				Series{
					Points: []Point{{V: 0, T: 0}, {V: 4, T: 25000}, {V: 10, T: 50000}},
					Metric: lbls2,
				},
			},
		}, {
			query: `metric{job="1"} @ 50 + metric{job="1"} @ 100`,
			start: 25,
			result: Vector{
				Sample{Point: Point{V: 15, T: 25000}, Metric: labels.FromStrings("job", "1")},
			},
		}, {
			query: `rate(metric{job="1"}[100s] @ 100) + label_replace(rate(metric{job="2"}[123s] @ 200), "job", "1", "", "")`,
			start: 25,
			result: Vector{
				Sample{Point: Point{V: 0.3, T: 25000}, Metric: labels.FromStrings("job", "1")},
			},
		}, {
			query: `sum_over_time(metric{job="1"}[100s] @ 100) 
                      + 
                    label_replace(sum_over_time(metric{job="2"}[100s] @ 100), "job", "1", "", "")`,
			start: 25,
			result: Vector{
				Sample{Point: Point{V: 165, T: 25000}, Metric: labels.FromStrings("job", "1")},
			},
		}, {
			query: `sum_over_time(metric{job="1"}[100s] @ 100) 
                      + 
                    label_replace(sum_over_time(metric{job="2"}[100s] @ 200), "job", "1", "", "")`,
			start: 25,
			result: Vector{
				Sample{Point: Point{V: 385, T: 25000}, Metric: labels.FromStrings("job", "1")},
			},
		}, {
			query: `sum_over_time(metric{job="1"}[100s] @ 100)[100s:25s] @ 50`,
			start: 100,
			result: Matrix{
				Series{
					Points: []Point{{V: 55, T: -50000}, {V: 55, T: -25000}, {V: 55, T: 0}, {V: 55, T: 25000}, {V: 55, T: 50000}},
					Metric: labels.FromStrings("job", "1"),
				},
			},
		}, {
			query: `sum_over_time(sum_over_time(metric{job="1"}[100s] @ 100)[100s:25s] @ 50)`,
			start: 100,
			result: Vector{
				Sample{Point: Point{V: 275, T: 100000}, Metric: labels.FromStrings("job", "1")},
			},
		}, {
			query: `sum_over_time(sum_over_time(metric{job="1"}[100s] @ 100)[100s:25s] @ 50)[3s:1s] @ 3000`,
			start: 100,
			result: Matrix{
				Series{
					Points: []Point{{V: 275, T: 2997000}, {V: 275, T: 2998000}, {V: 275, T: 2999000}, {V: 275, T: 3000000}},
					Metric: labels.FromStrings("job", "1"),
				},
			},
		}, { // timestamp() takes the time of the sample and not the evaluation time.
			query: `timestamp(((metric{job="2"} @ 50)))`,
			start: 100, end: 102,
			result: Matrix{
				Series{
					Points: []Point{{V: 50, T: 100000}, {V: 50, T: 101000}, {V: 50, T: 102000}},
					Metric: labels.FromStrings("job", "2"),
				},
			},
		}, {
			query: `minute()`,
			start: 30, end: 210, interval: 60,
			result: Matrix{
				Series{
					Points: []Point{{V: 0, T: 30000}, {V: 1, T: 90000}, {V: 2, T: 150000}, {V: 3, T: 210000}},
					Metric: labels.Labels{},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			if c.interval == 0 {
				c.interval = 1
			}
			start, end, interval := time.Unix(c.start, 0), time.Unix(c.end, 0), time.Duration(c.interval)*time.Second
			var err error
			var qry Query
			if c.end == 0 {
				qry, err = test.QueryEngine().NewInstantQuery(test.Queryable(), c.query, start)
			} else {
				qry, err = test.QueryEngine().NewRangeQuery(test.Queryable(), c.query, start, end, interval)
			}
			require.NoError(t, err)

			res := qry.Exec(test.Context())
			require.NoError(t, res.Err)
			require.Equal(t, c.result, res.Value, "query %q failed", c.query)
		})
	}
}

func TestMaxQuerySamples(t *testing.T) {
	test, err := NewTest(t, `
load 10s
  metric 1 2
  bigmetric{a="1"} 1 2
  bigmetric{a="2"} 1 2
`)
	require.NoError(t, err)
	defer test.Close()

	err = test.Run()
	require.NoError(t, err)

	cases := []struct {
		Query      string
		MaxSamples int
		Result     Result
		Start      time.Time
		End        time.Time
		Interval   time.Duration
	}{
		// Instant queries.
		{
			Query:      "1",
			MaxSamples: 1,
			Result: Result{
				nil,
				Scalar{V: 1, T: 1000},
				nil},
			Start: time.Unix(1, 0),
		},
		{
			Query:      "1",
			MaxSamples: 0,
			Result: Result{
				ErrTooManySamples(env),
				nil,
				nil,
			},
			Start: time.Unix(1, 0),
		},
		{
			Query:      "metric",
			MaxSamples: 0,
			Result: Result{
				ErrTooManySamples(env),
				nil,
				nil,
			},
			Start: time.Unix(1, 0),
		},
		{
			Query:      "metric",
			MaxSamples: 1,
			Result: Result{
				nil,
				Vector{
					Sample{Point: Point{V: 1, T: 1000},
						Metric: labels.FromStrings("__name__", "metric")},
				},
				nil,
			},
			Start: time.Unix(1, 0),
		},
		{
			Query:      "metric[20s]",
			MaxSamples: 2,
			Result: Result{
				nil,
				Matrix{Series{
					Points: []Point{{V: 1, T: 0}, {V: 2, T: 10000}},
					Metric: labels.FromStrings("__name__", "metric")},
				},
				nil,
			},
			Start: time.Unix(10, 0),
		},
		{
			Query:      "rate(metric[20s])",
			MaxSamples: 3,
			Result: Result{
				nil,
				Vector{
					Sample{
						Point:  Point{V: 0.1, T: 10000},
						Metric: labels.Labels{},
					},
				},
				nil,
			},
			Start: time.Unix(10, 0),
		},
		{
			Query:      "metric[20s:5s]",
			MaxSamples: 3,
			Result: Result{
				nil,
				Matrix{Series{
					Points: []Point{{V: 1, T: 0}, {V: 1, T: 5000}, {V: 2, T: 10000}},
					Metric: labels.FromStrings("__name__", "metric")},
				},
				nil,
			},
			Start: time.Unix(10, 0),
		},
		{
			Query:      "metric[20s]",
			MaxSamples: 0,
			Result: Result{
				ErrTooManySamples(env),
				nil,
				nil,
			},
			Start: time.Unix(10, 0),
		},
		// Range queries.
		{
			Query:      "1",
			MaxSamples: 3,
			Result: Result{
				nil,
				Matrix{Series{
					Points: []Point{{V: 1, T: 0}, {V: 1, T: 1000}, {V: 1, T: 2000}},
					Metric: labels.FromStrings()},
				},
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(2, 0),
			Interval: time.Second,
		},
		{
			Query:      "1",
			MaxSamples: 0,
			Result: Result{
				ErrTooManySamples(env),
				nil,
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(2, 0),
			Interval: time.Second,
		},
		{
			Query:      "metric",
			MaxSamples: 3,
			Result: Result{
				nil,
				Matrix{Series{
					Points: []Point{{V: 1, T: 0}, {V: 1, T: 1000}, {V: 1, T: 2000}},
					Metric: labels.FromStrings("__name__", "metric")},
				},
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(2, 0),
			Interval: time.Second,
		},
		{
			Query:      "metric",
			MaxSamples: 2,
			Result: Result{
				ErrTooManySamples(env),
				nil,
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(2, 0),
			Interval: time.Second,
		},
		{
			Query:      "metric",
			MaxSamples: 3,
			Result: Result{
				nil,
				Matrix{Series{
					Points: []Point{{V: 1, T: 0}, {V: 1, T: 5000}, {V: 2, T: 10000}},
					Metric: labels.FromStrings("__name__", "metric")},
				},
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(10, 0),
			Interval: 5 * time.Second,
		},
		{
			Query:      "metric",
			MaxSamples: 2,
			Result: Result{
				ErrTooManySamples(env),
				nil,
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(10, 0),
			Interval: 5 * time.Second,
		},
		{
			Query:      "rate(bigmetric[1s])",
			MaxSamples: 1,
			Result: Result{
				nil,
				Matrix{},
				nil,
			},
			Start:    time.Unix(0, 0),
			End:      time.Unix(10, 0),
			Interval: 5 * time.Second,
		},
	}

	engine := test.QueryEngine()
	for _, c := range cases {
		var err error
		var qry Query

		engine.maxSamplesPerQuery = c.MaxSamples

		if c.Interval == 0 {
			qry, err = engine.NewInstantQuery(test.Queryable(), c.Query, c.Start)
		} else {
			qry, err = engine.NewRangeQuery(test.Queryable(), c.Query, c.Start, c.End, c.Interval)
		}
		require.NoError(t, err)

		res := qry.Exec(test.Context())
		require.Equal(t, c.Result.Err, res.Err)
		require.Equal(t, c.Result.Value, res.Value, "query %q failed", c.Query)
	}
}

func TestRecoverEvaluatorRuntime(t *testing.T) {
	ev := &evaluator{logger: log.NewNopLogger()}

	var err error
	defer ev.recover(nil, &err)

	// Cause a runtime panic.
	var a []int
	//nolint:govet
	a[123] = 1

	if err.Error() != "unexpected error" {
		t.Fatalf("wrong error message: %q, expected %q", err, "unexpected error")
	}
}

func TestRecoverEvaluatorError(t *testing.T) {
	ev := &evaluator{logger: log.NewNopLogger()}
	var err error

	e := errors.New("custom error")

	defer func() {
		if err.Error() != e.Error() {
			t.Fatalf("wrong error message: %q, expected %q", err, e)
		}
	}()
	defer ev.recover(nil, &err)

	panic(e)
}

func TestRecoverEvaluatorErrorWithWarnings(t *testing.T) {
	ev := &evaluator{logger: log.NewNopLogger()}
	var err error
	var ws storage.Warnings

	warnings := storage.Warnings{errors.New("custom warning")}
	e := errWithWarnings{
		err:      errors.New("custom error"),
		warnings: warnings,
	}

	defer func() {
		if err.Error() != e.Error() {
			t.Fatalf("wrong error message: %q, expected %q", err, e)
		}
		if len(ws) != len(warnings) && ws[0] != warnings[0] {
			t.Fatalf("wrong warning message: %q, expected %q", ws[0], warnings[0])
		}
	}()
	defer ev.recover(&ws, &err)

	panic(e)
}

func TestSubquerySelector(t *testing.T) {
	type caseType struct {
		Query  string
		Result Result
		Start  time.Time
	}

	for _, tst := range []struct {
		loadString string
		cases      []caseType
	}{
		{
			loadString: `load 10s
							metric 1 2`,
			cases: []caseType{
				{
					Query: "metric[20s:10s]",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 1, T: 0}, {V: 2, T: 10000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(10, 0),
				},
				{
					Query: "metric[20s:5s]",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 1, T: 0}, {V: 1, T: 5000}, {V: 2, T: 10000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(10, 0),
				},
				{
					Query: "metric[20s:5s] offset 2s",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 1, T: 0}, {V: 1, T: 5000}, {V: 2, T: 10000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(12, 0),
				},
				{
					Query: "metric[20s:5s] offset 6s",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 1, T: 0}, {V: 1, T: 5000}, {V: 2, T: 10000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(20, 0),
				},
				{
					Query: "metric[20s:5s] offset 4s",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 2, T: 15000}, {V: 2, T: 20000}, {V: 2, T: 25000}, {V: 2, T: 30000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(35, 0),
				},
				{
					Query: "metric[20s:5s] offset 5s",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 2, T: 10000}, {V: 2, T: 15000}, {V: 2, T: 20000}, {V: 2, T: 25000}, {V: 2, T: 30000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(35, 0),
				},
				{
					Query: "metric[20s:5s] offset 6s",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 2, T: 10000}, {V: 2, T: 15000}, {V: 2, T: 20000}, {V: 2, T: 25000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(35, 0),
				},
				{
					Query: "metric[20s:5s] offset 7s",
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 2, T: 10000}, {V: 2, T: 15000}, {V: 2, T: 20000}, {V: 2, T: 25000}},
							Metric: labels.FromStrings("__name__", "metric")},
						},
						nil,
					},
					Start: time.Unix(35, 0),
				},
			},
		},
		{
			loadString: `load 10s
							http_requests{job="api-server", instance="0", group="production"}	0+10x1000 100+30x1000
							http_requests{job="api-server", instance="1", group="production"}	0+20x1000 200+30x1000
							http_requests{job="api-server", instance="0", group="canary"}		0+30x1000 300+80x1000
							http_requests{job="api-server", instance="1", group="canary"}		0+40x2000`,
			cases: []caseType{
				{ // Normal selector.
					Query: `http_requests{group=~"pro.*",instance="0"}[30s:10s]`,
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 9990, T: 9990000}, {V: 10000, T: 10000000}, {V: 100, T: 10010000}, {V: 130, T: 10020000}},
							Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production")},
						},
						nil,
					},
					Start: time.Unix(10020, 0),
				},
				{ // Default step.
					Query: `http_requests{group=~"pro.*",instance="0"}[5m:]`,
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 9840, T: 9840000}, {V: 9900, T: 9900000}, {V: 9960, T: 9960000}, {V: 130, T: 10020000}, {V: 310, T: 10080000}},
							Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production")},
						},
						nil,
					},
					Start: time.Unix(10100, 0),
				},
				{ // Checking if high offset (>LookbackDelta) is being taken care of.
					Query: `http_requests{group=~"pro.*",instance="0"}[5m:] offset 20m`,
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 8640, T: 8640000}, {V: 8700, T: 8700000}, {V: 8760, T: 8760000}, {V: 8820, T: 8820000}, {V: 8880, T: 8880000}},
							Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production")},
						},
						nil,
					},
					Start: time.Unix(10100, 0),
				},
				{
					Query: `rate(http_requests[1m])[15s:5s]`,
					Result: Result{
						nil,
						Matrix{
							Series{
								Points: []Point{{V: 3, T: 7985000}, {V: 3, T: 7990000}, {V: 3, T: 7995000}, {V: 3, T: 8000000}},
								Metric: labels.FromStrings("job", "api-server", "instance", "0", "group", "canary"),
							},
							Series{
								Points: []Point{{V: 4, T: 7985000}, {V: 4, T: 7990000}, {V: 4, T: 7995000}, {V: 4, T: 8000000}},
								Metric: labels.FromStrings("job", "api-server", "instance", "1", "group", "canary"),
							},
							Series{
								Points: []Point{{V: 1, T: 7985000}, {V: 1, T: 7990000}, {V: 1, T: 7995000}, {V: 1, T: 8000000}},
								Metric: labels.FromStrings("job", "api-server", "instance", "0", "group", "production"),
							},
							Series{
								Points: []Point{{V: 2, T: 7985000}, {V: 2, T: 7990000}, {V: 2, T: 7995000}, {V: 2, T: 8000000}},
								Metric: labels.FromStrings("job", "api-server", "instance", "1", "group", "production"),
							},
						},
						nil,
					},
					Start: time.Unix(8000, 0),
				},
				{
					Query: `sum(http_requests{group=~"pro.*"})[30s:10s]`,
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 270, T: 90000}, {V: 300, T: 100000}, {V: 330, T: 110000}, {V: 360, T: 120000}},
							Metric: labels.Labels{}},
						},
						nil,
					},
					Start: time.Unix(120, 0),
				},
				{
					Query: `sum(http_requests)[40s:10s]`,
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 800, T: 80000}, {V: 900, T: 90000}, {V: 1000, T: 100000}, {V: 1100, T: 110000}, {V: 1200, T: 120000}},
							Metric: labels.Labels{}},
						},
						nil,
					},
					Start: time.Unix(120, 0),
				},
				{
					Query: `(sum(http_requests{group=~"p.*"})+sum(http_requests{group=~"c.*"}))[20s:5s]`,
					Result: Result{
						nil,
						Matrix{Series{
							Points: []Point{{V: 1000, T: 100000}, {V: 1000, T: 105000}, {V: 1100, T: 110000}, {V: 1100, T: 115000}, {V: 1200, T: 120000}},
							Metric: labels.Labels{}},
						},
						nil,
					},
					Start: time.Unix(120, 0),
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			test, err := NewTest(t, tst.loadString)
			require.NoError(t, err)
			defer test.Close()

			require.NoError(t, test.Run())
			engine := test.QueryEngine()
			for _, c := range tst.cases {
				t.Run(c.Query, func(t *testing.T) {
					qry, err := engine.NewInstantQuery(test.Queryable(), c.Query, c.Start)
					require.NoError(t, err)

					res := qry.Exec(test.Context())
					require.Equal(t, c.Result.Err, res.Err)
					mat := res.Value.(Matrix)
					sort.Sort(mat)
					require.Equal(t, c.Result.Value, mat)
				})
			}
		})
	}
}

type FakeQueryLogger struct {
	closed bool
	logs   []interface{}
}

func NewFakeQueryLogger() *FakeQueryLogger {
	return &FakeQueryLogger{
		closed: false,
		logs:   make([]interface{}, 0),
	}
}

func (f *FakeQueryLogger) Close() error {
	f.closed = true
	return nil
}

func (f *FakeQueryLogger) Log(l ...interface{}) error {
	f.logs = append(f.logs, l...)
	return nil
}

func TestQueryLogger_basic(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    10 * time.Second,
	}
	engine := NewEngine(opts)

	queryExec := func() {
		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()
		query := engine.newTestQuery(func(ctx context.Context) error {
			return contextDone(ctx, "test statement execution")
		})
		res := query.Exec(ctx)
		require.NoError(t, res.Err)
	}

	// Query works without query log initialized.
	queryExec()

	f1 := NewFakeQueryLogger()
	engine.SetQueryLogger(f1)
	queryExec()
	for i, field := range []interface{}{"params", map[string]interface{}{"query": "test statement"}} {
		require.Equal(t, field, f1.logs[i])
	}

	l := len(f1.logs)
	queryExec()
	require.Equal(t, 2*l, len(f1.logs))

	// Test that we close the query logger when unsetting it.
	require.False(t, f1.closed, "expected f1 to be open, got closed")
	engine.SetQueryLogger(nil)
	require.True(t, f1.closed, "expected f1 to be closed, got open")
	queryExec()

	// Test that we close the query logger when swapping.
	f2 := NewFakeQueryLogger()
	f3 := NewFakeQueryLogger()
	engine.SetQueryLogger(f2)
	require.False(t, f2.closed, "expected f2 to be open, got closed")
	queryExec()
	engine.SetQueryLogger(f3)
	require.True(t, f2.closed, "expected f2 to be closed, got open")
	require.False(t, f3.closed, "expected f3 to be open, got closed")
	queryExec()
}

func TestQueryLogger_fields(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    10 * time.Second,
	}
	engine := NewEngine(opts)

	f1 := NewFakeQueryLogger()
	engine.SetQueryLogger(f1)

	ctx, cancelCtx := context.WithCancel(context.Background())
	ctx = NewOriginContext(ctx, map[string]interface{}{"foo": "bar"})
	defer cancelCtx()
	query := engine.newTestQuery(func(ctx context.Context) error {
		return contextDone(ctx, "test statement execution")
	})

	res := query.Exec(ctx)
	require.NoError(t, res.Err)

	expected := []string{"foo", "bar"}
	for i, field := range expected {
		v := f1.logs[len(f1.logs)-len(expected)+i].(string)
		require.Equal(t, field, v)
	}
}

func TestQueryLogger_error(t *testing.T) {
	opts := EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10,
		Timeout:    10 * time.Second,
	}
	engine := NewEngine(opts)

	f1 := NewFakeQueryLogger()
	engine.SetQueryLogger(f1)

	ctx, cancelCtx := context.WithCancel(context.Background())
	ctx = NewOriginContext(ctx, map[string]interface{}{"foo": "bar"})
	defer cancelCtx()
	testErr := errors.New("failure")
	query := engine.newTestQuery(func(ctx context.Context) error {
		return testErr
	})

	res := query.Exec(ctx)
	require.Error(t, res.Err, "query should have failed")

	for i, field := range []interface{}{"params", map[string]interface{}{"query": "test statement"}, "error", testErr} {
		require.Equal(t, f1.logs[i], field)
	}
}

var testExpr = []struct {
	input    string      // The input to be parsed.
	expected parser.Expr // The expected expression AST.
	fail     bool        // Whether parsing is supposed to fail.
	errMsg   string      // If not empty the parsing error has to contain this string.
}{
	{
		input: "123.4567",
		expected: &parser.NumberLiteral{
			Val:      123.4567,
			PosRange: parser.PositionRange{Start: 0, End: 8},
		},
	}, {
		input: "1 + 1",
		expected: &parser.BinaryExpr{
			Op: parser.ADD,
			LHS: &parser.NumberLiteral{
				Val:      1,
				PosRange: parser.PositionRange{Start: 0, End: 1},
			},
			RHS: &parser.NumberLiteral{
				Val:      1,
				PosRange: parser.PositionRange{Start: 4, End: 5},
			},
		},
	}, {
		input: "foo * bar",
		expected: &parser.BinaryExpr{
			Op: parser.MUL,
			LHS: &parser.VectorSelector{
				Name: "foo",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo"),
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   3,
				},
			},
			RHS: &parser.VectorSelector{
				Name: "bar",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "bar"),
				},
				PosRange: parser.PositionRange{
					Start: 6,
					End:   9,
				},
			},
			VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
		},
	}, {
		input: "foo * bar @ 10",
		expected: &parser.BinaryExpr{
			Op: parser.MUL,
			LHS: &parser.VectorSelector{
				Name: "foo",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo"),
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   3,
				},
			},
			RHS: &parser.StepInvariantExpr{
				Expr: &parser.VectorSelector{
					Name: "bar",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "bar"),
					},
					PosRange: parser.PositionRange{
						Start: 6,
						End:   14,
					},
					Timestamp: makeInt64Pointer(10000),
				},
			},
			VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
		},
	}, {
		input: "foo @ 20 * bar @ 10",
		expected: &parser.StepInvariantExpr{
			Expr: &parser.BinaryExpr{
				Op: parser.MUL,
				LHS: &parser.VectorSelector{
					Name: "foo",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   8,
					},
					Timestamp: makeInt64Pointer(20000),
				},
				RHS: &parser.VectorSelector{
					Name: "bar",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "bar"),
					},
					PosRange: parser.PositionRange{
						Start: 11,
						End:   19,
					},
					Timestamp: makeInt64Pointer(10000),
				},
				VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
			},
		},
	}, {
		input: "test[5s]",
		expected: &parser.MatrixSelector{
			VectorSelector: &parser.VectorSelector{
				Name: "test",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "test"),
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   4,
				},
			},
			Range:  5 * time.Second,
			EndPos: 8,
		},
	}, {
		input: `test{a="b"}[5y] @ 1603774699`,
		expected: &parser.StepInvariantExpr{
			Expr: &parser.MatrixSelector{
				VectorSelector: &parser.VectorSelector{
					Name:      "test",
					Timestamp: makeInt64Pointer(1603774699000),
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, "a", "b"),
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "test"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   11,
					},
				},
				Range:  5 * 365 * 24 * time.Hour,
				EndPos: 28,
			},
		},
	}, {
		input: "sum by (foo)(some_metric)",
		expected: &parser.AggregateExpr{
			Op: parser.SUM,
			Expr: &parser.VectorSelector{
				Name: "some_metric",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
				},
				PosRange: parser.PositionRange{
					Start: 13,
					End:   24,
				},
			},
			Grouping: []string{"foo"},
			PosRange: parser.PositionRange{
				Start: 0,
				End:   25,
			},
		},
	}, {
		input: "sum by (foo)(some_metric @ 10)",
		expected: &parser.StepInvariantExpr{
			Expr: &parser.AggregateExpr{
				Op: parser.SUM,
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
					},
					PosRange: parser.PositionRange{
						Start: 13,
						End:   29,
					},
					Timestamp: makeInt64Pointer(10000),
				},
				Grouping: []string{"foo"},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   30,
				},
			},
		},
	}, {
		input: "sum(some_metric1 @ 10) + sum(some_metric2 @ 20)",
		expected: &parser.StepInvariantExpr{
			Expr: &parser.BinaryExpr{
				Op:             parser.ADD,
				VectorMatching: &parser.VectorMatching{}, // TODO(codesome): why does it require this?
				LHS: &parser.AggregateExpr{
					Op: parser.SUM,
					Expr: &parser.VectorSelector{
						Name: "some_metric1",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric1"),
						},
						PosRange: parser.PositionRange{
							Start: 4,
							End:   21,
						},
						Timestamp: makeInt64Pointer(10000),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   22,
					},
				},
				RHS: &parser.AggregateExpr{
					Op: parser.SUM,
					Expr: &parser.VectorSelector{
						Name: "some_metric2",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric2"),
						},
						PosRange: parser.PositionRange{
							Start: 29,
							End:   46,
						},
						Timestamp: makeInt64Pointer(20000),
					},
					PosRange: parser.PositionRange{
						Start: 25,
						End:   47,
					},
				},
			},
		},
	}, {
		input: "some_metric and topk(5, rate(some_metric[1m] @ 20))",
		expected: &parser.BinaryExpr{
			Op: parser.LAND,
			VectorMatching: &parser.VectorMatching{
				Card: parser.CardManyToMany,
			},
			LHS: &parser.VectorSelector{
				Name: "some_metric",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   11,
				},
			},
			RHS: &parser.StepInvariantExpr{
				Expr: &parser.AggregateExpr{
					Op: parser.TOPK,
					Expr: &parser.Call{
						Func: parser.MustGetFunction("rate"),
						Args: parser.Expressions{
							&parser.MatrixSelector{
								VectorSelector: &parser.VectorSelector{
									Name: "some_metric",
									LabelMatchers: []*labels.Matcher{
										parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
									},
									PosRange: parser.PositionRange{
										Start: 29,
										End:   40,
									},
									Timestamp: makeInt64Pointer(20000),
								},
								Range:  1 * time.Minute,
								EndPos: 49,
							},
						},
						PosRange: parser.PositionRange{
							Start: 24,
							End:   50,
						},
					},
					Param: &parser.NumberLiteral{
						Val: 5,
						PosRange: parser.PositionRange{
							Start: 21,
							End:   22,
						},
					},
					PosRange: parser.PositionRange{
						Start: 16,
						End:   51,
					},
				},
			},
		},
	}, {
		input: "time()",
		expected: &parser.Call{
			Func: parser.MustGetFunction("time"),
			Args: parser.Expressions{},
			PosRange: parser.PositionRange{
				Start: 0,
				End:   6,
			},
		},
	}, {
		input: `foo{bar="baz"}[10m:6s]`,
		expected: &parser.SubqueryExpr{
			Expr: &parser.VectorSelector{
				Name: "foo",
				LabelMatchers: []*labels.Matcher{
					parser.MustLabelMatcher(labels.MatchEqual, "bar", "baz"),
					parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo"),
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   14,
				},
			},
			Range:  10 * time.Minute,
			Step:   6 * time.Second,
			EndPos: 22,
		},
	}, {
		input: `min_over_time(rate(foo{bar="baz"}[2s])[5m:] @ 1603775091)[4m:3s]`,
		expected: &parser.SubqueryExpr{
			Expr: &parser.StepInvariantExpr{
				Expr: &parser.Call{
					Func: parser.MustGetFunction("min_over_time"),
					Args: parser.Expressions{
						&parser.SubqueryExpr{
							Expr: &parser.Call{
								Func: parser.MustGetFunction("rate"),
								Args: parser.Expressions{
									&parser.MatrixSelector{
										VectorSelector: &parser.VectorSelector{
											Name: "foo",
											LabelMatchers: []*labels.Matcher{
												parser.MustLabelMatcher(labels.MatchEqual, "bar", "baz"),
												parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo"),
											},
											PosRange: parser.PositionRange{
												Start: 19,
												End:   33,
											},
										},
										Range:  2 * time.Second,
										EndPos: 37,
									},
								},
								PosRange: parser.PositionRange{
									Start: 14,
									End:   38,
								},
							},
							Range:     5 * time.Minute,
							Timestamp: makeInt64Pointer(1603775091000),
							EndPos:    56,
						},
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   57,
					},
				},
			},
			Range:  4 * time.Minute,
			Step:   3 * time.Second,
			EndPos: 64,
		},
	}, {
		input: `some_metric @ 123 offset 1m [10m:5s]`,
		expected: &parser.SubqueryExpr{
			Expr: &parser.StepInvariantExpr{
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   27,
					},
					Timestamp:      makeInt64Pointer(123000),
					Offset:         1 * time.Minute,
					OriginalOffset: 1 * time.Minute,
				},
			},
			Range:  10 * time.Minute,
			Step:   5 * time.Second,
			EndPos: 36,
		},
	}, {
		input: `some_metric[10m:5s] offset 1m @ 123`,
		expected: &parser.StepInvariantExpr{
			Expr: &parser.SubqueryExpr{
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   11,
					},
				},
				Timestamp:      makeInt64Pointer(123000),
				Offset:         1 * time.Minute,
				OriginalOffset: 1 * time.Minute,
				Range:          10 * time.Minute,
				Step:           5 * time.Second,
				EndPos:         35,
			},
		},
	}, {
		input: `(foo + bar{nm="val"} @ 1234)[5m:] @ 1603775019`,
		expected: &parser.StepInvariantExpr{
			Expr: &parser.SubqueryExpr{
				Expr: &parser.ParenExpr{
					Expr: &parser.BinaryExpr{
						Op: parser.ADD,
						VectorMatching: &parser.VectorMatching{
							Card: parser.CardOneToOne,
						},
						LHS: &parser.VectorSelector{
							Name: "foo",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "foo"),
							},
							PosRange: parser.PositionRange{
								Start: 1,
								End:   4,
							},
						},
						RHS: &parser.StepInvariantExpr{
							Expr: &parser.VectorSelector{
								Name: "bar",
								LabelMatchers: []*labels.Matcher{
									parser.MustLabelMatcher(labels.MatchEqual, "nm", "val"),
									parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "bar"),
								},
								Timestamp: makeInt64Pointer(1234000),
								PosRange: parser.PositionRange{
									Start: 7,
									End:   27,
								},
							},
						},
					},
					PosRange: parser.PositionRange{
						Start: 0,
						End:   28,
					},
				},
				Range:     5 * time.Minute,
				Timestamp: makeInt64Pointer(1603775019000),
				EndPos:    46,
			},
		},
	}, {
		input: "abs(abs(metric @ 10))",
		expected: &parser.StepInvariantExpr{
			Expr: &parser.Call{
				Func: &parser.Function{
					Name:       "abs",
					ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
					ReturnType: parser.ValueTypeVector,
				},
				Args: parser.Expressions{&parser.Call{
					Func: &parser.Function{
						Name:       "abs",
						ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
						ReturnType: parser.ValueTypeVector,
					},
					Args: parser.Expressions{&parser.VectorSelector{
						Name: "metric",
						LabelMatchers: []*labels.Matcher{
							parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
						},
						PosRange: parser.PositionRange{
							Start: 8,
							End:   19,
						},
						Timestamp: makeInt64Pointer(10000),
					}},
					PosRange: parser.PositionRange{
						Start: 4,
						End:   20,
					},
				}},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   21,
				},
			},
		},
	}, {
		input: "sum(sum(some_metric1 @ 10) + sum(some_metric2 @ 20))",
		expected: &parser.StepInvariantExpr{
			Expr: &parser.AggregateExpr{
				Op: parser.SUM,
				Expr: &parser.BinaryExpr{
					Op:             parser.ADD,
					VectorMatching: &parser.VectorMatching{}, // TODO(codesome): why does it require this?
					LHS: &parser.AggregateExpr{
						Op: parser.SUM,
						Expr: &parser.VectorSelector{
							Name: "some_metric1",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric1"),
							},
							PosRange: parser.PositionRange{
								Start: 8,
								End:   25,
							},
							Timestamp: makeInt64Pointer(10000),
						},
						PosRange: parser.PositionRange{
							Start: 4,
							End:   26,
						},
					},
					RHS: &parser.AggregateExpr{
						Op: parser.SUM,
						Expr: &parser.VectorSelector{
							Name: "some_metric2",
							LabelMatchers: []*labels.Matcher{
								parser.MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric2"),
							},
							PosRange: parser.PositionRange{
								Start: 33,
								End:   50,
							},
							Timestamp: makeInt64Pointer(20000),
						},
						PosRange: parser.PositionRange{
							Start: 29,
							End:   52,
						},
					},
				},
				PosRange: parser.PositionRange{
					Start: 0,
					End:   52,
				},
			},
		},
	},
}

func TestWrapWithStepInvariantExpr(t *testing.T) {
	for _, test := range testExpr {
		t.Run(test.input, func(t *testing.T) {
			expr, err := parser.ParseExpr(test.input)
			require.NoError(t, err)
			expr = WrapWithStepInvariantExpr(expr)
			require.Equal(t, test.expected, expr, "error on input '%s'", test.input)
		})
	}
}

func makeInt64Pointer(val int64) *int64 {
	valp := new(int64)
	*valp = val
	return valp
}
