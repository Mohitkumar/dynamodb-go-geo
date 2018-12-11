package geo

import (
	"math"

	"github.com/golang/geo/s1"

	"github.com/golang/geo/r1"
	"github.com/golang/geo/s2"

	"strconv"
)

//HashFromLatLong generate hash from latitude longitude
func HashFromLatLong(latitude float64, longitude float64) uint64 {
	var latLng = s2.LatLngFromDegrees(latitude, longitude)
	cell := s2.CellFromLatLng(latLng)
	return uint64(cell.ID())
}

//HashKey get hashkey from hash
func HashKey(hash uint64, keyLenght int) uint64 {
	if hash < 0 {
		keyLenght++
	}

	hashStr := strconv.FormatUint(hash, 10)
	denominator := uint64(math.Pow(10, float64(len(hashStr)-keyLenght)))
	if denominator == 0 {
		return hash
	}

	return hash / denominator

}

//BoundingBoxRect return bounding box by radius
func BoundingBoxRect(latitude float64, longitude float64, radius float64) s2.Rect {

	center := s2.LatLngFromDegrees(latitude, longitude)
	refUnitLat := 1.0
	refUnitLong := 1.0
	if latitude > 0.0 {
		refUnitLat = -1.0
	}
	if longitude > 0.0 {
		refUnitLong = -1.0
	}

	latRefLatLng := s2.LatLngFromDegrees(latitude+refUnitLat, longitude)
	longRefLatLong := s2.LatLngFromDegrees(latitude, longitude+refUnitLong)
	latForRadius := radius / EarthDistance(center, latRefLatLng)
	longForRadius := radius / EarthDistance(center, longRefLatLong)

	minLatLng := s2.LatLngFromDegrees(latitude-latForRadius, longitude-longForRadius)
	maxLatLng := s2.LatLngFromDegrees(latitude+latForRadius, longitude+longForRadius)

	return s2.Rect{Lat: r1.Interval{Lo: minLatLng.Lat.Radians(), Hi: maxLatLng.Lat.Radians()}, Lng: s1.Interval{Lo: minLatLng.Lng.Radians(), Hi: maxLatLng.Lng.Radians()}}
}

func EarthDistance(latLng1, latLng2 s2.LatLng) float64 {
	return latLng1.Distance(latLng2).Radians() * 6367000.0
}

//FindCellIds find all cell ids from rect
func FindCellIds(latLngRect s2.Rect) s2.CellUnion {
	var queue []s2.CellID
	var cellIds []s2.CellID

	for c := begin(0); c != end(0); c = c.Next() {
		if containsCellID(c, latLngRect) {
			queue = append(queue, c)
		}
	}
	processQueue(&queue, &cellIds, latLngRect)

	if len(cellIds) > 0 {
		cellUnion := s2.CellUnion(cellIds)
		return cellUnion
	}
	return nil
}

func begin(level int) s2.CellID {
	cellID := s2.CellID((uint64(0) << 61) + (uint64(0) | 1)).Parent(0).ChildBeginAtLevel(level)
	return cellID
}

func end(level int) s2.CellID {
	cellID := s2.CellID((uint64(5) << 61) + (uint64(0) | 1)).Parent(0).ChildBeginAtLevel(level)
	return cellID
}

func containsCellID(cellID s2.CellID, latLngRect s2.Rect) bool {
	return latLngRect.IntersectsCell(s2.CellFromCellID(cellID))
}

func processQueue(queue *[]s2.CellID, cellids *[]s2.CellID, latlngRect s2.Rect) {
	for len(*queue) > 0 {
		elem := (*queue)[0]
		*queue = (*queue)[1:]
		if !elem.IsValid() {
			break
		}
		processChildren(elem, latlngRect, queue, cellids)
	}
}

func processChildren(parent s2.CellID, latLngRect s2.Rect, queue *[]s2.CellID, cellids *[]s2.CellID) {
	children := make([]s2.CellID, 4)
	index := 0
	for c := parent.ChildBegin(); c != parent.ChildEnd(); c = c.Next() {
		if containsCellID(c, latLngRect) {
			children[index] = c
			index++
		}
	}
	if len(children) == 1 || len(children) == 2 {
		for _, child := range children {
			if child.IsLeaf() {
				*cellids = append(*cellids, child)
			} else {
				*queue = append(*queue, child)
			}
		}
	} else if len(children) == 3 {
		*cellids = append(*cellids, children...)
	} else if len(children) == 4 {
		*cellids = append(*cellids, parent)
	}
}
