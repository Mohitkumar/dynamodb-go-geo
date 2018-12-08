package geo

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//Config provides the way to build ge request
type Config struct {
	GeoIndexName     string
	GeoHashKeyColumn string
	GeoHashColumn    string
	GeoHashKeyLenght string
}

//PutItem override PutItem to add geopoint
func PutItem(putItemRequest dynamodb.PutItemInput, latitude float64, longitude float64, config Config) dynamodb.PutItemInput {
	return putItemRequest
}
