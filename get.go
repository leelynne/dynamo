package dynamo

import (
	"fmt"

	"github.com/crowdmob/goamz/dynamodb"
)

type getOptions struct{}

// GetOpts namespaces the GetRequest options
var GetOpts = getOptions{}

type GetRequest struct {
	consistentReads bool
}

func (d Dynamo) Get(table, key string, val interface{}, options ...func(*GetRequest) error) error {
	fullkey := keyParts{id: key}
	if d.compoundKeys {
		fullkey = parseDataKey(key)
	}
	tb, _, err := d.getTable(table)
	if err != nil {
		return err
	}

	attrs, err := tb.GetItem(&dynamodb.Key{
		HashKey:  fullkey.id,
		RangeKey: fullkey.scope,
	})

	if err != nil {
		if err == dynamodb.ErrNotFound {
			return fmt.Errorf("Item '%s:%s not found.'", table, key)
		}
		return fmt.Errorf("Failed to get item '%s:%s - %s'", table, key, err)
	}
	err = dynamodb.UnmarshalAttributes(&attrs, val)
	if err != nil {
		return fmt.Errorf("Failed unmarshal on item '%s:%s' with attributes %s - %s", table, key, attrs, err)
	}

	return nil

}
