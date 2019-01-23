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
	"math/rand"
	"sort"

	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

// GenSeries generates series with a given number of labels and values.
func GenSeries(totalSeries, labelCount int, mint, maxt int64) []tsdb.Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}
	series := make([]tsdb.Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		for len(lbls) < labelCount {
			lbls[RandString()] = RandString()
		}
		samples := make([]Sample, 0, maxt-mint+1)
		for t := mint; t <= maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series[i] = NewSeries(lbls, samples)
	}

	return series
}

type mockSeries struct {
	labels   func() labels.Labels
	iterator func() tsdb.SeriesIterator
}

func NewSeries(l map[string]string, s []Sample) tsdb.Series {
	return &mockSeries{
		labels:   func() labels.Labels { return labels.FromMap(l) },
		iterator: func() tsdb.SeriesIterator { return newListSeriesIterator(s) },
	}
}
func (m *mockSeries) Labels() labels.Labels         { return m.labels() }
func (m *mockSeries) Iterator() tsdb.SeriesIterator { return m.iterator() }

type listSeriesIterator struct {
	list []Sample
	idx  int
}

func newListSeriesIterator(list []Sample) *listSeriesIterator {
	return &listSeriesIterator{list: list, idx: -1}
}

func (it *listSeriesIterator) At() (int64, float64) {
	s := it.list[it.idx]
	return s.T(), s.V()
}

func (it *listSeriesIterator) Next() bool {
	it.idx++
	return it.idx < len(it.list)
}

func (it *listSeriesIterator) Seek(t int64) bool {
	if it.idx == -1 {
		it.idx = 0
	}
	// Do binary search between current position and end.
	it.idx = sort.Search(len(it.list)-it.idx, func(i int) bool {
		s := it.list[i+it.idx]
		return s.T() >= t
	})

	return it.idx < len(it.list)
}

func (it *listSeriesIterator) Err() error {
	return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// RandString generates random string.
func RandString() string {
	maxLength := int32(50)
	length := rand.Int31n(maxLength)
	b := make([]byte, length+1)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := length, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
