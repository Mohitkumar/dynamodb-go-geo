package geo

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//GenerateQueries generate queries
func GenerateQueries(queryRequest dynamodb.QueryInput, latitude float64, longitude float64, radius float64, config *Config) []dynamodb.QueryInput {
	outerHashRanges := HashRanges(latitude, longitude, radius)
	queryRequests := make([]dynamodb.QueryInput, 0)

	for _, outerHashRange := range outerHashRanges {
		innerhashRanges := outerHashRange.split(config.GeoHashKeyLenght)
		for _, innerHashrange := range innerhashRanges {
			queryRequestCopy := copyQueryInput(queryRequest)

			hashKey := HashKey(innerHashrange.RangeMin, config.GeoHashKeyLenght)
			hashKeystr := strconv.FormatUint(hashKey, 10)
			keyConditions := make(map[string]*dynamodb.Condition)
			attrValueList := make([]*dynamodb.AttributeValue, 0)
			attrValueList = append(attrValueList, &dynamodb.AttributeValue{N: aws.String(hashKeystr)})
			geoHashCondition := dynamodb.Condition{ComparisonOperator: aws.String("EQ"),
				AttributeValueList: attrValueList}
			keyConditions[config.GeoHashKeyColumn] = &geoHashCondition
			minRangeStr := strconv.FormatUint(innerHashrange.RangeMin, 10)
			maxRangeStr := strconv.FormatUint(innerHashrange.RangeMax, 10)
			minRange := dynamodb.AttributeValue{N: &minRangeStr}
			maxRange := dynamodb.AttributeValue{N: &maxRangeStr}

			geoHashCondition2 := dynamodb.Condition{ComparisonOperator: aws.String("BETWEEN"),
				AttributeValueList: []*dynamodb.AttributeValue{&minRange, &maxRange}}
			keyConditions[config.GeoHashColumn] = &geoHashCondition2
			queryRequestCopy.SetKeyConditions(keyConditions)
			queryRequestCopy.SetIndexName(config.GeoIndexName)
			queryRequests = append(queryRequests, queryRequestCopy)
		}
	}
	return queryRequests
}

func copyQueryInput(input dynamodb.QueryInput) dynamodb.QueryInput {
	copyInput := dynamodb.QueryInput{}
	copyInput.SetAttributesToGet(input.AttributesToGet)
	if input.ConsistentRead != nil {
		copyInput.SetConsistentRead(*input.ConsistentRead)
	}
	copyInput.SetExclusiveStartKey(input.ExclusiveStartKey)
	if input.IndexName != nil {
		copyInput.SetIndexName(*input.IndexName)
	}
	copyInput.SetKeyConditions(input.KeyConditions)
	if input.Limit != nil {
		copyInput.SetLimit(*input.Limit)
	}
	if input.ReturnConsumedCapacity != nil {
		copyInput.SetReturnConsumedCapacity(*input.ReturnConsumedCapacity)
	}
	if input.ScanIndexForward != nil {
		copyInput.SetScanIndexForward(*input.ScanIndexForward)
	}
	if input.Select != nil {
		copyInput.SetSelect(*input.Select)
	}
	if input.TableName != nil {
		copyInput.SetTableName(*input.TableName)
	}
	if input.FilterExpression != nil {
		copyInput.SetFilterExpression(*input.FilterExpression)
	}
	copyInput.SetExpressionAttributeNames(input.ExpressionAttributeNames)
	copyInput.SetExpressionAttributeValues(input.ExpressionAttributeValues)
	return copyInput
}
