package geo

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/geo/s2"
)

//QueryRequest query request
type QueryRequest struct {
	Queries []dynamodb.QueryInput
	Filters Filter
}

//RadiusQuery queries
func RadiusQuery(queryRequest dynamodb.QueryInput, latitude float64, longitude float64,
	radius float64, config *Config) QueryRequest {
	queryRequests := GenerateQueries(queryRequest, latitude, longitude, radius, config)

	centerLatLng := s2.LatLngFromDegrees(latitude, longitude)

	filter := Filter{center: centerLatLng, radius: radius}

	return QueryRequest{Filters: filter, Queries: queryRequests}
}
