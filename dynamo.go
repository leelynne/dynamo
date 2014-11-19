package dynamo

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
)

type Dynamo struct {
	auth            aws.Auth
	server          dynamodb.Server
	compoundKeys    bool
	consistentReads bool
	getTableName    ProcessTableName
}

// ProcessTableName allows manipulation of the table name queried.
// Since DynamoDB provides a single data namespace per AWS account this func can
// be used to add a namespace to tables, e.g. prod_users or test_users. The default
// implementation just returns the tableName given.
type ProcessTableName func(tableName string) string

// OptCompoundKeys Compound keys are a shortcut for range and a hash key in the form of "myrange/myhash".
// A key with '/' separating the range and hash.
func OptCompoundKeys(ck bool) func(*Dynamo) error {
	return func(d *Dynamo) error {
		d.compoundKeys = ck
		return nil
	}
}

// OptConsistentReads sets the default consistent read behavior. Can be overridden on a request basis. Defaults to true
func OptConsistentReads(cr bool) func(*Dynamo) error {
	return func(d *Dynamo) error {
		d.consistentReads = cr
		return nil
	}
}

func NewDynamo(region Region, options ...func(*Dynamo) error) (Dynamo, error) {
	if region == "" {
		return Dynamo{}, errors.New("No region/endpoint specified.")
	}
	var r aws.Region
	if strings.Contains(string(region), "http") {
		r = aws.Region{DynamoDBEndpoint: string(region)}

	} else {
		r = aws.Regions[string(region)]
		if r.DynamoDBEndpoint == "" {
			return Dynamo{}, fmt.Errorf("%s is not a valid region", region)
		}
	}

	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		return Dynamo{}, err
	}
	d := Dynamo{
		auth:            auth,
		server:          dynamodb.Server{Auth: auth, Region: r},
		compoundKeys:    true,
		consistentReads: true,
		getTableName:    func(t string) { return t },
	}
	return d, nil
}

func (d Dynamo) Save(table, key string, val interface{}) error {
	attrs, err := dynamodb.MarshalAttributes(val)
	if err != nil {
		return fmt.Errorf("Failed to marshall attributes: %s", err)
	}
	tb, _, err := d.getTable(table)
	if err != nil {
		return err
	}

	fullkey := keyParts{id: key}
	if d.compoundKeys {
		fullkey = parseDataKey(key)
	}
	_, err = tb.PutItem(fullkey.id, fullkey.scope, attrs)
	if err != nil {
		return fmt.Errorf("Put item failed for %s:%s - %s", table, key, err)
	}
	return nil
}

// scanSettings are the settings based on analyzing the tables
// size and configured throughput

func (d Dynamo) calcScanRoutines(tb *dynamodb.Table) int {
	return 2
}

func (d Dynamo) getTable(rawName string) (tbl *dynamodb.Table, itemCount int, err error) {
	tName := d.getTableName(rawName)
	// Having to run a describe table just to get a Table instance is terrible
	tableDesc, err := d.server.DescribeTable(tName)
	itemCount = int(tableDesc.ItemCount)
	if err != nil {
		return nil, itemCount, fmt.Errorf("Failure describing table '%s - %s'.", tName, err)
	}
	pkey, err := tableDesc.BuildPrimaryKey()
	if err != nil {
		return nil, itemCount, fmt.Errorf("Failure getting primary key from table '%s - %s'", tName, err)
	}
	return d.server.NewTable(tName, pkey), itemCount, nil
}

type keyParts struct {
	scope string
	id    string
}

func parseDataKey(k string) keyParts {
	parts := strings.Split(string(k), "/")
	scope, id := "", ""

	if len(parts) == 1 {
		id = string(k)
	} else {
		scope = parts[0]
		id = parts[1]
	}
	return keyParts{
		scope: scope,
		id:    id,
	}
}
