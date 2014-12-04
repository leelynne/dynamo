package dynamo

import (
	"fmt"

	"github.com/crowdmob/goamz/dynamodb"
)

type ErrItemNotFound struct {
	msg string
}

func (e ErrItemNotFound) Error() string {
	return e.msg
}

// GetOpts namespaces the GetRequest options
var GetOpts = getOptions{}

type getOptions struct{}

type GetRequest struct {
	consistentReads bool
}

// OptConsistentReads sets consistent
func (getOptions) OptConsistentReads(c bool) func(*GetRequest) error {
	return func(gr *GetRequest) error {
		gr.consistentReads = c
		return nil
	}
}

func (d *Dynamo) Get(table, key string, val interface{}, options ...func(*GetRequest) error) error {
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
			return ErrItemNotFound{fmt.Sprintf("Item '%s:%s not found.'", table, key)}
		}
		return fmt.Errorf("Failed to get item '%s:%s - %s'", table, key, err)
	}
	err = dynamodb.UnmarshalAttributes(&attrs, val)
	if err != nil {
		return fmt.Errorf("Failed unmarshal on item '%s:%s' with attributes %s - %s", table, key, attrs, err)
	}

	return nil
}

func defaultGetReq() GetRequest {
	return GetRequest{
		consistentReads: true,
	}
}

func (gr *GetRequest) applyOptions(options []func(*GetRequest) error) error {
	for _, option := range options {
		err := option(gr)
		if err != nil {
			return err
		}
	}
	return nil
}
