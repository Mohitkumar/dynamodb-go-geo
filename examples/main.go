package main

import (
	"fmt"

	"github.com/Mohitkumar/dynamodb-go-geo/geo"
)

func main() {
	id := geo.HashFromLatLong(-30.043800, -51.140220)
	fmt.Println(id)
	fmt.Println(geo.HashKey(id, 4))
	fmt.Println(geo.BoundingBoxRect(-30.043800, -51.140220, 100))
}
