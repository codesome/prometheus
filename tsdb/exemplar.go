// Copyright 2020 The Prometheus Authors
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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type exemplarMetrics struct {
	outOfOrderExemplars prometheus.Counter
	duplicateExemplars  prometheus.Counter
}

func newExemplarMetrics(r prometheus.Registerer) *exemplarMetrics {
	m := &exemplarMetrics{
		outOfOrderExemplars: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_exemplar_out_of_order_exemplars_total",
			Help: "Total number of out of order samples ingestion failed attempts",
		}),
		duplicateExemplars: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_exemplar_duplicate_exemplars_total",
			Help: "Total number of series in the head block.",
		}),
	}
	if r != nil {
		r.MustRegister(
			m.outOfOrderExemplars,
			m.duplicateExemplars,
		)
	}
	return m
}

type CircularExemplarStorage struct {
	metrics     *exemplarMetrics
	lock        sync.RWMutex
	index       map[string]int
	exemplars   []*circularBufferEntry
	nextIndex   int
	secondaries []storage.ExemplarAppender
}

type circularBufferEntry struct {
	exemplar        exemplar.Exemplar
	seriesLabels    labels.Labels
	scrapeTimestamp int64
	prev            int // index of previous exemplar in circular for the same series, use -1 as a default for new entries
}

// If we assume the average case 95 bytes per exemplar we can fit 5651272 exemplars in
// 1GB of extra memory, accounting for the fact that this is heap allocated space.
func NewCircularExemplarStorage(len int, reg prometheus.Registerer, secondaries ...storage.ExemplarAppender) *CircularExemplarStorage {
	return &CircularExemplarStorage{
		exemplars:   make([]*circularBufferEntry, len),
		index:       make(map[string]int),
		secondaries: secondaries,
		metrics:     newExemplarMetrics(reg),
	}
}

func (ce *CircularExemplarStorage) Appender() storage.ExemplarAppender {
	return ce
}

// TODO: separate wrapper struct for queries?
func (ce *CircularExemplarStorage) Querier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return ce, nil
}

func (ce *CircularExemplarStorage) SetSecondaries(secondaries ...storage.ExemplarAppender) {
	ce.secondaries = secondaries
}

// Select returns exemplars for a given set of series labels hash.
func (ce *CircularExemplarStorage) Select(start, end int64, l labels.Labels) ([]exemplar.Exemplar, error) {
	var (
		ret []exemplar.Exemplar
		e   exemplar.Exemplar
		idx int
		ok  bool
		buf []byte
	)

	ce.lock.RLock()
	defer ce.lock.RUnlock()

	if idx, ok = ce.index[l.String()]; !ok {
		return nil, nil
	}
	lastTs := ce.exemplars[idx].scrapeTimestamp

	for {
		// We need the labels check here in case what was the previous exemplar for the series
		// when the exemplar from the last loop iteration was written has since been overwritten
		// with an exemplar from another series.
		// todo (callum) confirm if this check is still needed now that adding an exemplar should
		// update the index and previous pointer for the series whose exemplar was overwritten.
		if idx == -1 || string(ce.exemplars[idx].seriesLabels.Bytes(buf)) != string(l.Bytes(buf)) {
			break
		}

		e = ce.exemplars[idx].exemplar
		// todo (callum) This line is needed to avoid an infinite loop, consider redesign of buffer entry struct.
		if ce.exemplars[idx].scrapeTimestamp > lastTs {
			break
		}

		lastTs = ce.exemplars[idx].scrapeTimestamp
		if e.Ts >= start && e.Ts <= end {
			ret = append(ret, e)
		}
		idx = ce.exemplars[idx].prev
	}
	reverseExemplars(ret)
	return ret, nil
}

// Takes the circularBufferEntry that will be overwritten and updates the
// storages index for that entries labelset if necessary.
func (ce *CircularExemplarStorage) indexGcCheck(cbe *circularBufferEntry) {
	if cbe == nil {
		return
	}

	l := cbe.seriesLabels
	i := cbe.prev
	if cbe.prev == -1 {
		delete(ce.index, l.String())
		return
	}

	if ce.exemplars[ce.nextIndex] != nil {
		l2 := ce.exemplars[i].seriesLabels
		if !labels.Equal(l2, l) { // No more exemplars for series l.
			delete(ce.index, cbe.seriesLabels.String())
			return
		}
		// There's still at least one exemplar for the series l, so we can update the index.
		ce.index[l.String()] = i
	}
}

func (ce *CircularExemplarStorage) addExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	seriesLabels := l.String()
	ce.lock.Lock()
	defer ce.lock.Unlock()
	idx, ok := ce.index[seriesLabels]

	if !ok {
		ce.indexGcCheck(ce.exemplars[ce.nextIndex])
		// Default the prev value to -1 (which we use to detect that we've iterated through all exemplars for a series in Select)
		// since this is the first exemplar stored for this series.
		ce.exemplars[ce.nextIndex] = &circularBufferEntry{
			exemplar:        e,
			seriesLabels:    l,
			scrapeTimestamp: t,
			prev:            -1}
		ce.index[seriesLabels] = ce.nextIndex
		ce.nextIndex++
		if ce.nextIndex >= cap(ce.exemplars) {
			ce.nextIndex = 0
		}
		return nil
	}

	// Check for duplicate vs last stored exemplar for this series.
	if ce.exemplars[idx].exemplar.Equals(e) {
		ce.metrics.duplicateExemplars.Inc()
		return storage.ErrDuplicateExemplar
	}
	if e.Ts <= ce.exemplars[idx].scrapeTimestamp || t <= ce.exemplars[idx].scrapeTimestamp {
		ce.metrics.outOfOrderExemplars.Inc()
		return storage.ErrOutOfOrderExemplar
	}
	ce.indexGcCheck(ce.exemplars[ce.nextIndex])
	ce.exemplars[ce.nextIndex] = &circularBufferEntry{
		exemplar:        e,
		seriesLabels:    l,
		scrapeTimestamp: t,
		prev:            idx}
	ce.index[seriesLabels] = ce.nextIndex
	ce.nextIndex++
	if ce.nextIndex >= cap(ce.exemplars) {
		ce.nextIndex = 0
	}
	return nil
}

func (ce *CircularExemplarStorage) AddExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	if err := ce.addExemplar(l, t, e); err != nil {
		return err
	}

	for _, s := range ce.secondaries {
		if err := s.AddExemplar(l, t, e); err != nil {
			return err
		}
	}
	return nil
}

// For use in tests, clears the entire exemplar storage.
func (ce *CircularExemplarStorage) Reset() {
	ce.exemplars = make([]*circularBufferEntry, len(ce.exemplars))
	ce.index = make(map[string]int)
}

func reverseExemplars(b []exemplar.Exemplar) {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
}
