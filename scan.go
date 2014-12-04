package dynamo

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"code.google.com/p/go.net/context"

	"github.com/crowdmob/goamz/dynamodb"
)

type scanOptions struct{}

// ScanOpts namespaces the ScanRequest options e.g. ScanOpts.MaxThroughput(30)
var ScanOpts = scanOptions{}

// ScanRequest holds options for a scan call.
// Set options with ScanOpts.Opt*
type ScanRequest struct {
	// MaxThroughputConsumption as a percent described as an int. e.g. 50, 25
	maxThroughputConsumption int
	itemBufferSize           int
	log                      *log.Logger
}

func defaultScanRequest() ScanRequest {
	return ScanRequest{
		maxThroughputConsumption: 0,
		itemBufferSize:           5000,
		log:                      log.New(os.Stdout, "dynascan", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (sr *ScanRequest) applyOptions(options []func(*ScanRequest) error) error {
	for _, option := range options {
		err := option(sr)
		if err != nil {
			return err
		}
	}
	return nil
}

// OptMaxThroughput sets the allowed max throughput for the scan operation to use.
func (scanOptions) OptMaxThroughput(percent int) func(*ScanRequest) error {
	return func(sr *ScanRequest) error {
		sr.maxThroughputConsumption = percent
		return nil
	}
}

// OptLogger sets the logger. // Defaults to stdout
func (scanOptions) OptLogger(l *log.Logger) func(*ScanRequest) error {
	return func(sr *ScanRequest) error {
		if l == nil {
			return errors.New("Logger cannot be nil")
		}
		sr.log = l
		return nil
	}
}

func (scanOptions) OptBufferSize(size int) func(*ScanRequest) error {
	return func(sr *ScanRequest) error {
		sr.itemBufferSize = size
		return nil
	}
}

// Unmarshal attempts to unmarshal the dynamo data into the given interface
type Unmarshal func(interface{}) error

// ScanItem is a single dynamo scan result
type ScanItem struct {
	// Unmarsh is a function that will run dynamo unmarshalling code on the attributes
	Unmarsh Unmarshal
	// Attributes are the 'raw' dynamodb attributes if needed. Most use cases can use the Unmarsh() function
	Attributes map[string]*dynamodb.Attribute
}

// Scan will get all rows from a dynamo table.
func (d Dynamo) Scan(ctx context.Context, table string, options ...func(*ScanRequest) error) (si chan ScanItem, e chan error, tableItemCount int, err error) {
	req := defaultScanRequest()
	err = req.applyOptions(options)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("Bad ScanRequest option %s.", err)
	}
	tbl, itemCount, err := d.getTable(table)
	if err != nil {
		return nil, nil, itemCount, err
	}
	routines := d.calcScanRoutines(tbl)
	unmarsh := make(chan ScanItem, req.itemBufferSize)
	errch := make(chan error, 1)
	wg := sync.WaitGroup{}

	for i := 0; i < routines; i++ {
		wg.Add(1)
		req.log.Printf("Scan segment %d starting", i)
		go func(segment int) {
			defer wg.Done()

			var lastKey *dynamodb.Key
			for {
				rawVals, nextKey, err := tbl.ParallelScanPartial([]dynamodb.AttributeComparison{}, lastKey, segment, routines)
				lastKey = nextKey
				if err != nil {
					select {
					case errch <- fmt.Errorf("Error starting parallel scan: %s", err):
						return
					case <-ctx.Done():
						return
					}
				}
				handled := 0
				for _, attrs := range rawVals {
					si := ScanItem{
						Unmarsh: func(v interface{}) error {
							return dynamodb.UnmarshalAttributes(&attrs, v)
						},
						Attributes: attrs,
					}
					select {
					case unmarsh <- si:
					case <-ctx.Done():
						return
					}
					handled++
				}
				if lastKey == nil {
					break
				}
			}
			req.log.Printf("Scan segment %d done", segment)
		}(i)
	}
	go func() {
		wg.Wait()
		close(unmarsh)
		close(errch)
	}()

	return unmarsh, errch, itemCount, nil
}
