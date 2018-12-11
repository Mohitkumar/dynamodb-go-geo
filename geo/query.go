package geo

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/geo/s2"
)

//QueryRequest query request
type QueryRequest struct {
	queries []dynamodb.QueryInput
	filter  Filter
}

//RadiusQuery queries
func RadiusQuery(queryRequest dynamodb.QueryInput, latitude float64, longitude float64,
	radius float64, config *Config) QueryRequest {
	boundingBox := BoundingBoxRect(latitude, longitude, radius)
	queryRequests := GenerateQueries(queryRequest, boundingBox, config)

	centerLatLng := s2.LatLngFromDegrees(latitude, longitude)

	filter := Filter{center: centerLatLng, radius: radius}

	return QueryRequest{filter: filter, queries: queryRequests}
}
