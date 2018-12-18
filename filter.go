package geo

import (
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/geo/s2"
)

//Filter interface
type Filter struct {
	center s2.LatLng
	radius float64
}

//FilterItems return filters
func (filter Filter) FilterItems(items []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	result := make([]map[string]*dynamodb.AttributeValue, 0)
	for _, attrMap := range items {
		latitude := extractField(attrMap, "latitude")
		longitude := extractField(attrMap, "longitude")

		if latitude != 0 && longitude != 0 {
			latLng := s2.LatLngFromDegrees(latitude, longitude)
			distance := EarthDistance(filter.center, latLng)
			if distance < filter.radius {
				result = append(result, attrMap)
			}
		}
	}
	return result
}

func extractField(item map[string]*dynamodb.AttributeValue, f string) float64 {
	field := item[f]
	if field == nil {
		return 0
	}
	v, err := strconv.ParseFloat(*field.N, 64)
	if err != nil {
		return 0
	}
	return v
}
