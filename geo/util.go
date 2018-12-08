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
	latForRadius := radius / earthDistance(center, latRefLatLng)
	longForRadius := radius / earthDistance(center, longRefLatLong)

	minLatLng := s2.LatLngFromDegrees(latitude-latForRadius, longitude-longForRadius)
	maxLatLng := s2.LatLngFromDegrees(latitude+latForRadius, longitude+longForRadius)

	return s2.Rect{Lat: r1.Interval{Lo: minLatLng.Lat.Radians(), Hi: maxLatLng.Lat.Radians()}, Lng: s1.Interval{Lo: minLatLng.Lng.Radians(), Hi: maxLatLng.Lng.Radians()}}
}

func earthDistance(latLng1, latLng2 s2.LatLng) float64 {
	return latLng1.Distance(latLng2).Radians() * 6367000.0
}
