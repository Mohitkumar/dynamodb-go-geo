package geo

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"strconv"
)

//Config provides the way to build ge request
type Config struct {
	GeoIndexName     string
	GeoHashKeyColumn string
	GeoHashColumn    string
	GeoHashKeyLenght int
}

//QueryClient build client
type QueryClient struct {
	Service *dynamodb.DynamoDB
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

//Execute execute the geo queries
func (client QueryClient) Execute(queryRequest QueryRequest) []map[string]*dynamodb.AttributeValue {
	result := make([]map[string]*dynamodb.AttributeValue, 0)
	ch := make(chan []map[string]*dynamodb.AttributeValue)
	for _, queryInput := range queryRequest.Queries {
		go executeQuery(client, queryInput, queryRequest.Filters, ch)
		res := <-ch
		result = append(result, res...)
	}
	close(ch)
	return result
}

func executeQuery(client QueryClient, queryInput dynamodb.QueryInput, filter Filter, ch chan []map[string]*dynamodb.AttributeValue) {
	result := make([]map[string]*dynamodb.AttributeValue, 0)
	svc := client.Service
	if svc != nil {
		for {
			output, err := svc.Query(&queryInput)
			if err == nil {
				items := output.Items
				filterdItems := filter.FilterItems(items)
				result = append(result, filterdItems...)
				queryInput = *queryInput.SetExclusiveStartKey(output.LastEvaluatedKey)
				if output.LastEvaluatedKey == nil {
					break
				}
			} else {
				break
			}
		}
	}
	ch <- result
}
