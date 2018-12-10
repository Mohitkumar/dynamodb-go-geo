package geo

import (
	"github.com/golang/geo/s2"
)

//HashRanges get all ranges
func HashRanges(rect s2.Rect) []HashRange {
	cellids := FindCellIds(rect)
	return mergCells(cellids)
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
				break
				wasMerged = true
			}
		}
		if !wasMerged {
			ranges = append(ranges, hashRange)
		}
	}
	return ranges
}
