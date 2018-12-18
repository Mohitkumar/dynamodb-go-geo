package geo

import (
	"math"

	"github.com/golang/geo/s1"

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

//EarthDistance returns earth distance
func EarthDistance(latLng1, latLng2 s2.LatLng) float64 {
	return latLng1.Distance(latLng2).Radians() * 6371000.0
}

//NearbyCellIds find all cell ids within readius
func NearbyCellIds(latitude float64, longitude float64, radius float64) []s2.CellID {
	p := s2.PointFromLatLng(s2.LatLngFromDegrees(latitude, longitude))
	angle := s1.Angle(radius / 6371000.0)
	cap := s2.CapFromCenterAngle(p, angle)
	region := s2.Region(cap)

	rc := &s2.RegionCoverer{MaxLevel: 20, MinLevel: 9}
	cellUnion := rc.Covering(region)
	return []s2.CellID(cellUnion)
}
