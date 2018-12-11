package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/Mohitkumar/dynamodb-go-geo/geo"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type ItemInfo struct {
	Country string `json:"country"`
}

type Item struct {
	ID   int64    `json:"id"`
	Info ItemInfo `json:"info"`
}

func main() {
	//id := geo.HashFromLatLong(-30.043800, -51.140220)
	//fmt.Println(id)
	//fmt.Println(geo.HashKey(id, 4))
	//fmt.Println(geo.BoundingBoxRect(-30.043800, -51.140220, 100))
	//createTable()
	//testPutItem()
	testQuery()
	//testUtils()
}

func createTable() {
	sess, err := session.NewSession(&aws.Config{Region: aws.String("mumbai"), Endpoint: aws.String("http://localhost:8000")})
	if err != nil {
		fmt.Println("can not create table")
	}

	input := &dynamodb.CreateTableInput{
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("geoHashKey"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("geoHash"),
				AttributeType: aws.String("N"),
			},
		},

		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("geoHashKey"),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String("geoHash"),
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
				IndexName: aws.String("User_gsi"),
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: aws.Int64(10),
				},
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String("User"),
	}

	svc := dynamodb.New(sess)
	_, err = svc.CreateTable(input)

	if err != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Println("Created the table User")
}

func testPutItem() {
	info := ItemInfo{
		Country: "India",
	}

	item := Item{
		ID:   1234,
		Info: info,
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("can not marshal", err)
	}

	input := dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("User"),
	}

	config := &geo.Config{
		GeoIndexName:     "User_gsi",
		GeoHashColumn:    "geoHash",
		GeoHashKeyColumn: "geoHashKey",
		GeoHashKeyLenght: 4,
	}
	newInput, err := geo.PutItem(input, -30.043800, -51.140220, config)
	if err != nil {
		fmt.Println("can not create put item", err)
	}
	fmt.Println(newInput)
	sess, err := session.NewSession(&aws.Config{Region: aws.String("mumbai"), Endpoint: aws.String("http://localhost:8000")})
	svc := dynamodb.New(sess)
	_, err = svc.PutItem(&newInput)

	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Println("Successfully added record to table")

}

func testQuery() {
	sess, err := session.NewSession(&aws.Config{Region: aws.String("mumbai"), Endpoint: aws.String("http://localhost:8000")})
	if err != nil {
		fmt.Println("can not connect")
		return
	}
	svc := dynamodb.New(sess)
	client := geo.QueryClient{Service: svc}

	query := &dynamodb.QueryInput{
		TableName: aws.String("User"),
	}

	config := &geo.Config{
		GeoIndexName:     "User_gsi",
		GeoHashColumn:    "geoHash",
		GeoHashKeyColumn: "geoHashKey",
		GeoHashKeyLenght: 4,
	}

	radiusQuery := geo.RadiusQuery(*query, -30.043800, -51.140220, 100000, config)
	//fmt.Println(radiusQuery)
	result := client.Execute(radiusQuery)
	fmt.Println(result)
	for _, res := range result {
		fmt.Println(res["id"].S)
	}
}

func testUtils() {
	rect := geo.BoundingBoxRect(-30.043800, -51.140220, 10*1000)
	fmt.Println(rect)
	//cellUnion := geo.FindCellIds(rect)
	//fmt.Println(cellUnion)
	ranges := geo.HashRanges(rect)
	fmt.Println(ranges)

}
