package delayhist

import (
	"fmt"
	"time"
)

var DefaultUpperBounds = []time.Duration{
	10 * time.Millisecond,
	20 * time.Millisecond,
	30 * time.Millisecond,
	40 * time.Millisecond,
	50 * time.Millisecond,
	60 * time.Millisecond,
	70 * time.Millisecond,
	80 * time.Millisecond,
	90 * time.Millisecond,
	100 * time.Millisecond,
	150 * time.Millisecond,
	200 * time.Millisecond,
}

type Bucket struct {
	Range           string `json:"range"`
	LowerBound      string `json:"lower_bound"`
	LowerBoundNanos int64  `json:"lower_bound_nanos"`
	UpperBound      string `json:"upper_bound,omitempty"`
	UpperBoundNanos int64  `json:"upper_bound_nanos,omitempty"`
	Count           uint64 `json:"count"`
}

func NewCounts() []uint64 {
	return make([]uint64, len(DefaultUpperBounds)+2)
}

func Add(counts []uint64, delay time.Duration) []uint64 {
	if len(counts) < len(DefaultUpperBounds)+2 {
		next := NewCounts()
		copy(next, counts)
		counts = next
	}
	counts[BucketIndex(delay)]++
	return counts
}

func BucketIndex(delay time.Duration) int {
	if delay <= 0 {
		return 0
	}
	for i, upperBound := range DefaultUpperBounds {
		if delay <= upperBound {
			return i + 1
		}
	}
	return len(DefaultUpperBounds) + 1
}

func Snapshot(counts []uint64) []Bucket {
	histogram := make([]Bucket, 0, len(DefaultUpperBounds)+2)
	zeroCount := uint64(0)
	if len(counts) > 0 {
		zeroCount = counts[0]
	}
	histogram = append(histogram, Bucket{
		Range:           "0s",
		LowerBound:      "0s",
		LowerBoundNanos: 0,
		UpperBound:      "0s",
		UpperBoundNanos: 0,
		Count:           zeroCount,
	})

	lowerBound := time.Duration(0)
	for i, upperBound := range DefaultUpperBounds {
		count := uint64(0)
		countIndex := i + 1
		if countIndex < len(counts) {
			count = counts[countIndex]
		}

		histogram = append(histogram, Bucket{
			Range:           fmt.Sprintf("(%s,%s]", lowerBound, upperBound),
			LowerBound:      lowerBound.String(),
			LowerBoundNanos: int64(lowerBound),
			UpperBound:      upperBound.String(),
			UpperBoundNanos: int64(upperBound),
			Count:           count,
		})
		lowerBound = upperBound
	}

	overflowCount := uint64(0)
	overflowIndex := len(DefaultUpperBounds) + 1
	if len(counts) > overflowIndex {
		overflowCount = counts[overflowIndex]
	}
	histogram = append(histogram, Bucket{
		Range:           ">" + lowerBound.String(),
		LowerBound:      lowerBound.String(),
		LowerBoundNanos: int64(lowerBound),
		Count:           overflowCount,
	})
	return histogram
}
