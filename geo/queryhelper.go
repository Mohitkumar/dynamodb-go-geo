package geo

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/geo/s2"
)

//HashRanges get all ranges
func HashRanges(rect s2.Rect) []HashRange {
	cellids := FindCellIds(rect)
	return mergCells(cellids)
}

func mergCells(cellUniun s2.CellUnion) []HashRange {
	cellIds := []s2.CellID(cellUniun)
	ranges := make([]HashRange, len(cellIds))
	for _, cellID := range cellIds {
		hashRange := NewHashRange(uint64(cellID.RangeMin()), uint64(cellID.RangeMax()))
		wasMerged := false

		for _, r := range ranges {
			merged := r.merge(hashRange)
			if merged {
				break
				wasMerged = true
			}
		}
		if !wasMerged {
			ranges = append(ranges, hashRange)
		}
	}
	return ranges
}

//GenerateQueries generate queries
func GenerateQueries(queryRequest dynamodb.QueryInput, boundingBox s2.Rect, config *Config) []dynamodb.QueryInput {
	outerHashRanges := HashRanges(boundingBox)
	queryRequests := make([]dynamodb.QueryInput, len(outerHashRanges))

	for _, outerHashRange := range outerHashRanges {
		innerhashRanges := outerHashRange.split(config.GeoHashKeyLenght)
		for _, innerHashrange := range innerhashRanges {
			queryRequestCopy := copyQueryInput(queryRequest)

			hashKey := HashKey(innerHashrange.RangeMin, config.GeoHashKeyLenght)
			hashKeystr := strconv.FormatUint(hashKey, 10)
			keyConditions := make(map[string]*dynamodb.Condition)
			attrValueList := make([]*dynamodb.AttributeValue, 1)
			attrValueList = append(attrValueList, &dynamodb.AttributeValue{N: &hashKeystr})
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
	copyInput.SetConsistentRead(*input.ConsistentRead)
	copyInput.SetExclusiveStartKey(input.ExclusiveStartKey)
	copyInput.SetIndexName(*input.IndexName)
	copyInput.SetKeyConditions(input.KeyConditions)
	copyInput.SetLimit(*input.Limit)
	copyInput.SetReturnConsumedCapacity(*input.ReturnConsumedCapacity)
	copyInput.SetScanIndexForward(*input.ScanIndexForward)
	copyInput.SetSelect(*input.Select)
	copyInput.SetTableName(*input.TableName)
	copyInput.SetFilterExpression(*input.FilterExpression)
	copyInput.SetExpressionAttributeNames(input.ExpressionAttributeNames)
	copyInput.SetExpressionAttributeValues(input.ExpressionAttributeValues)
	return copyInput
}
