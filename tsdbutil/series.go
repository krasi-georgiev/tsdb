// Copyright 2013 The Prometheus Authors
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

package tsdbutil

import (
	"fmt"

	"github.com/prometheus/tsdb/labels"
)

func GenSeries(numLabels, numVals int) []labels.Labels {
	labelPrefix := "label-"
	valuePrefix := "value-"
	vals := make([]int, numLabels)
	permuts := &([][]int{})
	GenSeriesRec(0, numLabels, numVals, vals, permuts)
	series := make([]labels.Labels, 0)
	for _, vals := range *permuts {
		l := labels.Labels{}
		for i, v := range vals {
			l = append(l, labels.Label{
				Name:  fmt.Sprintf("%s%d", labelPrefix, i),
				Value: fmt.Sprintf("%s%d", valuePrefix, v),
			})
		}
		series = append(series, l)
	}
	return series
}

func GenSeriesRec(idx, numLabels, numVals int, vals []int, series *[][]int) {
	if idx == numLabels {
		vals2 := make([]int, len(vals))
		copy(vals2, vals)
		*series = append(*series, vals2)
		return
	}
	for i := 0; i < numVals; i++ {
		vals[idx] = i
		GenSeriesRec(idx+1, numLabels, numVals, vals, series)
	}
}
