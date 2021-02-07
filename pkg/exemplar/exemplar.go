// Copyright 2019 The Prometheus Authors
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

package exemplar

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"

	"github.com/prometheus/prometheus/pkg/labels"
)

// Exemplar is additional information associated with a time series.
type Exemplar struct {
	Labels labels.Labels `json:"labels"`
	Value  float64       `json:"value"`
	Ts     int64         `json:"timestamp"`
	HasTs  bool          `json:"-"`
}

// Equals compares if the exemplar e is the same as e2.
func (e Exemplar) Equals(e2 Exemplar) bool {
	if e.Labels.String() != e2.Labels.String() {
		return false
	}

	if e.Ts != e2.Ts {
		return false
	}

	if e.Value != e2.Value {
		return false
	}

	return true
}

func (e Exemplar) MarshalJSON() ([]byte, error) {
	var nts bytes.Buffer
	partial := int(e.Ts / 1000)
	fraction := int(e.Ts % 1000)

	nts.Write([]byte(strconv.Itoa(partial)))
	if fraction != 0 {
		nts.WriteRune('.')
		if fraction < 100 {
			nts.WriteRune('0')

		}
		if fraction < 10 {
			nts.WriteRune('0')
		}
		nts.Write([]byte(strconv.Itoa(fraction)))
	}

	f, err := strconv.ParseFloat(nts.String(), 64)
	if err != nil {
		return nil, err
	}

	abs := math.Abs(e.Value)
	fmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmt = 'e'
		}
	}
	nts.Reset()
	b := nts.Bytes()
	b = strconv.AppendFloat(b, e.Value, fmt, -1, 64)

	return json.Marshal(&struct {
		Labels labels.Labels `json:"labels"`
		Value  string        `json:"value"`
		Ts     float64       `json:"timestamp"`
	}{
		Labels: e.Labels,
		Value:  string(b),
		Ts:     f,
	})
}
