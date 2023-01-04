// Copyright 2022 The Prometheus Authors
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

package histogram

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBound(t *testing.T) {
	scenarios := []struct {
		idx    int32
		schema int32
		want   float64
	}{
		{
			idx:    -1,
			schema: -1,
			want:   0.25,
		},
		{
			idx:    0,
			schema: -1,
			want:   1,
		},
		{
			idx:    1,
			schema: -1,
			want:   4,
		},
		{
			idx:    512,
			schema: -1,
			want:   math.MaxFloat64,
		},
		{
			idx:    513,
			schema: -1,
			want:   math.Inf(+1),
		},
		{
			idx:    -1,
			schema: 0,
			want:   0.5,
		},
		{
			idx:    0,
			schema: 0,
			want:   1,
		},
		{
			idx:    1,
			schema: 0,
			want:   2,
		},
		{
			idx:    1024,
			schema: 0,
			want:   math.MaxFloat64,
		},
		{
			idx:    1025,
			schema: 0,
			want:   math.Inf(+1),
		},
		{
			idx:    -1,
			schema: 2,
			want:   0.8408964152537144,
		},
		{
			idx:    0,
			schema: 2,
			want:   1,
		},
		{
			idx:    1,
			schema: 2,
			want:   1.189207115002721,
		},
		{
			idx:    4096,
			schema: 2,
			want:   math.MaxFloat64,
		},
		{
			idx:    4097,
			schema: 2,
			want:   math.Inf(+1),
		},
	}

	for _, s := range scenarios {
		got := getBound(s.idx, s.schema)
		if s.want != got {
			require.Equal(t, s.want, got, "idx %d, schema %d", s.idx, s.schema)
		}
	}
}

func TestUnionOfSpans(t *testing.T) {
	cases := []struct {
		s1, s2, exp []Span
	}{
		{
			// Has the cases of
			// 1.  |----|        (partial overlap)
			//        |----|
			//
			// 2.       |-----|  (no gap but no overlap as well)
			//     |---|
			//
			// 3.  |----|        (complete overlap)
			//     |----|
			s1: []Span{
				{0, 3},
				{3, 3},
				{5, 3},
			},
			s2: []Span{
				{0, 2},
				{2, 2},
				{2, 3},
				{3, 3},
			},
			exp: []Span{
				{0, 3},
				{1, 7},
				{3, 3},
			},
		},
		{
			// s1 is superset of s2.
			s1: []Span{
				{0, 3},
				{3, 5},
				{3, 3},
			},
			s2: []Span{
				{0, 2},
				{5, 3},
				{4, 3},
			},
			exp: []Span{
				{0, 3},
				{3, 5},
				{3, 3},
			},
		},
		{
			// No overlaps but one span is side by side.
			s1: []Span{
				{0, 3},
				{3, 3},
				{5, 3},
			},
			s2: []Span{
				{3, 3},
				{4, 2},
			},
			exp: []Span{
				{0, 9},
				{1, 2},
				{2, 3},
			},
		},
		{
			// No buckets in one of them.
			s1: []Span{
				{0, 3},
				{3, 3},
				{5, 3},
			},
			exp: []Span{
				{0, 3},
				{3, 3},
				{5, 3},
			},
		},
		{ // Zero length spans.
			s1: []Span{
				{-5, 0},
				{2, 0},
				{3, 3},
				{1, 0},
				{2, 3},
				{2, 0},
				{2, 0},
				{1, 3},
				{4, 0},
				{5, 0},
			},
			s2: []Span{
				{0, 2},
				{2, 2},
				{1, 0},
				{1, 3},
				{3, 3},
			},
			exp: []Span{
				{0, 3},
				{1, 7},
				{3, 3},
			},
		},
	}

	for _, c := range cases {
		s1c := make([]Span, len(c.s1))
		s2c := make([]Span, len(c.s2))
		copy(s1c, c.s1)
		copy(s2c, c.s2)

		require.Equal(t, c.exp, UnionOfSpans(c.s1, c.s2))
		require.Equal(t, c.exp, UnionOfSpans(s2c, s1c))
	}
}
