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

package rules

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

func TestGroup_RetryStateRestoration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expr, err := testParser.ParseExpr("up == 0")
		require.NoError(t, err)
		rule := NewAlertingRule("Down", expr, time.Minute, 0, labels.EmptyLabels(), labels.EmptyLabels(), labels.EmptyLabels(), "", false, nil)
		attempts := 0
		querier := &storage.MockQuerier{SelectMockFunction: func(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
			return storage.EmptySeriesSet()
		}}
		group := NewGroup(GroupOptions{
			Name: "restore", Interval: time.Second, Rules: []Rule{rule}, ShouldRestore: true,
			EvalIterationFunc: func(context.Context, *Group, time.Time) {},
			Opts: &ManagerOptions{Context: context.Background(), Queryable: storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
				attempts++
				if attempts == 1 {
					return nil, tsdb.ErrNotReady
				}
				return querier, nil
			})},
		})
		go group.run(context.Background())
		time.Sleep(5 * time.Second)
		group.stop()
		require.Equal(t, 2, attempts)
		require.True(t, rule.Restored())
		require.False(t, group.shouldRestore)
	})
}

func TestGroup_Equals(t *testing.T) {
	tests := map[string]struct {
		first    *Group
		second   *Group
		expected bool
	}{
		"no query offset set on both groups": {
			first: &Group{
				name:     "group-1",
				file:     "file-1",
				interval: time.Minute,
			},
			second: &Group{
				name:     "group-1",
				file:     "file-1",
				interval: time.Minute,
			},
			expected: true,
		},
		"query offset set only on the first group": {
			first: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			second: &Group{
				name:     "group-1",
				file:     "file-1",
				interval: time.Minute,
			},
			expected: false,
		},
		"query offset set on both groups to the same value": {
			first: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			second: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			expected: true,
		},
		"query offset set on both groups to different value": {
			first: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](time.Minute),
			},
			second: &Group{
				name:        "group-1",
				file:        "file-1",
				interval:    time.Minute,
				queryOffset: pointerOf[time.Duration](2 * time.Minute),
			},
			expected: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expected, testData.first.Equals(testData.second))
			require.Equal(t, testData.expected, testData.second.Equals(testData.first))
		})
	}
}

func pointerOf[T any](value T) *T {
	return &value
}
