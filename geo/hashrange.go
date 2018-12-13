package geo

import (
	"math"
	"strconv"

	"github.com/golang/geo/s2"
)

//HashRange represet a range of hash
type HashRange struct {
	RangeMin        uint64
	RangeMax        uint64
	mergeThreashold int
}

//NewHashRange create new hash range
func NewHashRange(range1 uint64, range2 uint64) HashRange {
	hashRange := HashRange{}
	hashRange.RangeMin = uint64(math.Min(float64(range1), float64(range2)))
	hashRange.RangeMax = uint64(math.Max(float64(range1), float64(range2)))
	hashRange.mergeThreashold = 2
	return hashRange
}

func HashRanges(latitude float64, longitude float64, radius float64) []HashRange {
	cellids := NearbyCellIds(latitude, longitude, radius)
	return createRanges(s2.CellUnion(cellids))
}

func createRanges(cellUniun s2.CellUnion) []HashRange {
	cellIds := []s2.CellID(cellUniun)
	ranges := make([]HashRange, 0)
	for _, cellID := range cellIds {
		hashRange := NewHashRange(uint64(cellID.RangeMin()), uint64(cellID.RangeMax()))
		ranges = append(ranges, hashRange)
	}
	return ranges
}

func mergCells(cellUniun s2.CellUnion) []HashRange {
	cellIds := []s2.CellID(cellUniun)
	ranges := make([]HashRange, len(cellIds))
	for _, cellID := range cellIds {
		hashRange := NewHashRange(uint64(cellID.RangeMin()), uint64(cellID.RangeMax()))
		wasMerged := false

		for _, r := range ranges {
			merged := r.merge(hashRange)
			if merged {
				wasMerged = true
				break
			}
		}
		if !wasMerged {
			ranges = append(ranges, hashRange)
		}
	}
	return ranges
}

func (hashRange HashRange) merge(hashRange2 HashRange) bool {
	if hashRange2.RangeMin-hashRange.RangeMax <= uint64(hashRange.mergeThreashold) && hashRange2.RangeMin-hashRange.RangeMax > 0 {
		hashRange.RangeMax = hashRange2.RangeMax
		return true
	}

	if hashRange.RangeMin-hashRange2.RangeMax <= uint64(hashRange.mergeThreashold) && hashRange.RangeMin-hashRange2.RangeMax > 0 {
		hashRange.RangeMin = hashRange2.RangeMin
		return true
	}
	return false
}

func (hashRange HashRange) split(hashKeyLength int) []HashRange {
	var result []HashRange
	minHashKey := HashKey(hashRange.RangeMin, hashKeyLength)
	maxHashKey := HashKey(hashRange.RangeMax, hashKeyLength)

	denominator := uint64(math.Pow(10, float64(len(strconv.FormatUint(hashRange.RangeMin, 10))-len(strconv.FormatUint(minHashKey, 10)))))
	if minHashKey == maxHashKey {
		result = append(result, hashRange)
	} else {
		for l := minHashKey; l <= maxHashKey; l++ {
			if l > 0 {
				min := hashRange.RangeMin
				if l != minHashKey {
					min = l * denominator
				}
				max := hashRange.RangeMax
				if l != maxHashKey {
					max = (l+1)*denominator - 1
				}
				rng := NewHashRange(min, max)
				result = append(result, rng)
			} else {
				min := hashRange.RangeMin
				if l != minHashKey {
					min = (l-1)*denominator + 1
				}
				max := hashRange.RangeMax
				if l != maxHashKey {
					max = l * denominator
				}
				rng := NewHashRange(min, max)
				result = append(result, rng)
			}
		}
	}
	return result
}
