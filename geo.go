package geo

import (
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"strconv"
)

const poolSize int = 100

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

//ExecuteAsync run all queries in async mode
func (client QueryClient) ExecuteAsync(queryRequest QueryRequest) []map[string]*dynamodb.AttributeValue {
	result := make([]map[string]*dynamodb.AttributeValue, 0)
	c := make(chan []map[string]*dynamodb.AttributeValue, len(queryRequest.Queries))
	pool := make(chan int, poolSize)
	var wg sync.WaitGroup
	for _, queryInput := range queryRequest.Queries {
		wg.Add(1)
		pool <- 1
		go func(queryInput dynamodb.QueryInput) {
			defer wg.Done()
			executeRoutine(client, queryInput, queryRequest.Filters, c)
			<-pool
		}(queryInput)

	}
	close(pool)
	go func() {
		wg.Wait()
		close(c)
	}()
	for res := range c {
		result = append(result, res...)
	}

	return result
}

func executeRoutine(client QueryClient, queryInput dynamodb.QueryInput, queryFilter Filter,
	c chan []map[string]*dynamodb.AttributeValue) {
	res := executeQuery(client, queryInput, queryFilter)
	c <- res

}

//Execute execute the geo queries
func (client QueryClient) Execute(queryRequest QueryRequest) []map[string]*dynamodb.AttributeValue {
	result := make([]map[string]*dynamodb.AttributeValue, 0)
	for _, queryInput := range queryRequest.Queries {
		res := executeQuery(client, queryInput, queryRequest.Filters)
		result = append(result, res...)
	}
	return result
}

func executeQuery(client QueryClient, queryInput dynamodb.QueryInput, filter Filter) []map[string]*dynamodb.AttributeValue {
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
	return result
}
