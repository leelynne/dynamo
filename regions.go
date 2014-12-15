package dynamo

// Region is the dynamo endpoint for an AWS Region
type Region struct {
	Name    string // Name is required for signing
	Address string
}

// US-West-2 region in Oregon
var USWest2 = Region{
	Name:    "us-west-2",
	Address: "https://dynamodb.us-west-2.amazonaws.com:443",
}
