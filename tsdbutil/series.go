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
	"time"

	"github.com/prometheus/tsdb/labels"
)

// GenSeries generates series with a given number of labels and values.
func GenSeries(totalSeries int, labelCount int, cardinality, churn bool) []labels.Labels {
	series := make([]labels.Labels, totalSeries)
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	labelNames := make([]string, labelCount)
	labelValues := make([]string, labelCount)

	// Generate all label names and values.
	for v := 0; v < labelCount; v++ {
		labelNames[v] = RandString()
		if !cardinality {
			labelValues[v] = RandString()
		}
	}

	for s := 0; s < totalSeries; s++ {
		lbs := labels.Labels{}
		lbsC := labelCount
		if churn {
			rand.Seed(time.Now().UnixNano())
			lbsC = rand.Intn(labelCount) + 1 // We don't want 0.
		}
		for i := 0; i < lbsC; i++ {
			l := labels.Label{
				Name:  labelNames[i],
				Value: labelValues[i],
			}
			if l.Value == "" {
				l.Value = RandString()
			}
			lbs = append(lbs, l)
		}
		series[s] = lbs
	}
	return series
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
