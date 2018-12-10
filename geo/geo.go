package geo

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"strconv"

	"github.com/golang/geo/s2"
)

//Config provides the way to build ge request
type Config struct {
	GeoIndexName     string
	GeoHashKeyColumn string
	GeoHashColumn    string
	GeoHashKeyLenght int
}

//PutItem override PutItem to add geopoint
func PutItem(putItemRequest dynamodb.PutItemInput, latitude float64, longitude float64, config *Config) (dynamodb.PutItemInput, error) {
	if config == nil {
		return putItemRequest, errors.New("config is null")
	}

	geoHash := HashFromLatLong(latitude, longitude)
	geoHashStr := strconv.FormatUint(geoHash, 10)
	geoHashkey := HashKey(geoHash, config.GeoHashKeyLenght)
	geoHashKeyStr := strconv.FormatUint(geoHashkey, 10)
	hashAttr := dynamodb.AttributeValue{N: &geoHashStr}
	attrValueMap := putItemRequest.Item
	attrValueMap[config.GeoHashColumn] = &hashAttr

	geoHashKeyAttr := dynamodb.AttributeValue{N: &geoHashKeyStr}
	attrValueMap[config.GeoHashKeyColumn] = &geoHashKeyAttr
	return putItemRequest, nil
}

//RadiusQuery queries
func RadiusQuery(queryRequest dynamodb.QueryInput, latitude float64, longitude float64, radius float64, config *Config) {
	boundingBox := BoundingBoxRect(latitude, longitude, radius)
	GenerateQueries(queryRequest, boundingBox, config)

	centerLatLng := s2.LatLngFromDegrees(latitude, longitude)
}
