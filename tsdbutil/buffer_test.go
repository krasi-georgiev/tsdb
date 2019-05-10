// Copyright 2018 The Prometheus Authors
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
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/prometheus/tsdb/testutil"
)

func TestSampleRing(t *testing.T) {
	cases := []struct {
		input []int64
		delta int64
		size  int
	}{
		{
			input: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			delta: 2,
			size:  1,
		},
		{
			input: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			delta: 2,
			size:  2,
		},
		{
			input: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			delta: 7,
			size:  3,
		},
		{
			input: []int64{1, 2, 3, 4, 5, 16, 17, 18, 19, 20},
			delta: 7,
			size:  1,
		},
	}

	val := make([]byte, 4)
	for _, c := range cases {
		r := newSampleRing(c.delta, c.size)

		input := []sample{}
		for _, t := range c.input {
			rand.Read(val)
			input = append(input, sample{
				t: t,
				v: val,
			})
		}

		for i, s := range input {
			r.add(s.t, s.v)
			buffered := r.samples()

			for _, sold := range input[:i] {
				found := false
				for _, bs := range buffered {
					if bs.t == sold.t && bytes.Equal(bs.v, sold.v) {
						found = true
						break
					}
				}
				if sold.t >= s.t-c.delta && !found {
					t.Fatalf("%d: expected sample %d to be in buffer but was not; buffer %v", i, sold.t, buffered)
				}
				if sold.t < s.t-c.delta && found {
					t.Fatalf("%d: unexpected sample %d in buffer; buffer %v", i, sold.t, buffered)
				}
			}
		}
	}
}

func TestBufferedSeriesIterator(t *testing.T) {
	var it *BufferedSeriesIterator

	bufferEq := func(exp []sample) {
		var b []sample
		bit := it.Buffer()
		for bit.Next() {
			t, v := bit.At()
			b = append(b, sample{t: t, v: v})
		}
		testutil.Equals(t, exp, b)
	}
	sampleEq := func(ets int64, ev string) {
		ts, v := it.At()
		testutil.Equals(t, ets, ts)
		testutil.Equals(t, []byte(ev), v)
	}

	it = NewBuffer(newListSeriesIterator([]sample{
		{t: 1, v: []byte("2")},
		{t: 2, v: []byte("3")},
		{t: 3, v: []byte("4")},
		{t: 4, v: []byte("5")},
		{t: 5, v: []byte("6")},
		{t: 99, v: []byte("8")},
		{t: 100, v: []byte("9")},
		{t: 101, v: []byte("10")},
	}), 2)

	testutil.Assert(t, it.Seek(-123) == true, "seek failed")
	sampleEq(1, "2")
	bufferEq(nil)

	testutil.Assert(t, it.Next() == true, "next failed")
	sampleEq(2, "3")
	bufferEq([]sample{{t: 1, v: []byte("2")}})

	testutil.Assert(t, it.Next() == true, "next failed")
	testutil.Assert(t, it.Next() == true, "next failed")
	testutil.Assert(t, it.Next() == true, "next failed")
	sampleEq(5, "6")
	bufferEq([]sample{{t: 2, v: []byte("3")}, {t: 3, v: []byte("4")}, {t: 4, v: []byte("5")}})

	testutil.Assert(t, it.Seek(5) == true, "seek failed")
	sampleEq(5, "6")
	bufferEq([]sample{{t: 2, v: []byte("3")}, {t: 3, v: []byte("4")}, {t: 4, v: []byte("5")}})

	testutil.Assert(t, it.Seek(101) == true, "seek failed")
	sampleEq(101, "10")
	bufferEq([]sample{{t: 99, v: []byte("8")}, {t: 100, v: []byte("9")}})

	testutil.Assert(t, it.Next() == false, "next succeeded unexpectedly")
}

type listSeriesIterator struct {
	list []sample
	idx  int
}

func newListSeriesIterator(list []sample) *listSeriesIterator {
	return &listSeriesIterator{list: list, idx: -1}
}

func (it *listSeriesIterator) At() (int64, []byte) {
	s := it.list[it.idx]
	return s.t, s.v
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
		return s.t >= t
	})

	return it.idx < len(it.list)
}

func (it *listSeriesIterator) Err() error {
	return nil
}
