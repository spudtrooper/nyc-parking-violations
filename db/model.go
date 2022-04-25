package db

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spudtrooper/goutil/check"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var (
	printUpdateTiming = flag.Bool("print_updating_timing", false, "print timing stats for updates")
)

type ResultState string

const (
	ResultsStateUnset ResultState = "unset"
	ResultStateError  ResultState = "error"
	ResultStateDone   ResultState = "done"
)

type plate struct {
	Value string
	State string
}

type storedResult struct {
	State     ResultState
	Error     string
	TotalOwed float64
}

type storedPlate struct {
	Plate  plate
	Result storedResult
	Tag    string
}

func isNoDocs(err error) bool {
	return strings.Contains(err.Error(), "no documents in result")
}

func (d *DB) plates() *mongo.Collection {
	return d.collection("plates")
}

func (d *DB) MustDebugString(ctx context.Context) (string, *DebugInfo) {
	res, dbg, err := d.DebugString(ctx)
	check.Err(err)
	return res, dbg
}

type DebugInfo struct {
	CountUnset int64
	CountDone  int64
	CountError int64
}

func (d *DB) DebugString(ctx context.Context) (string, *DebugInfo, error) {
	var buf bytes.Buffer

	var countUnset, countDone, countError int64
	{
		filter := bson.D{{"result.state", ResultsStateUnset}}
		cnt, err := d.plates().CountDocuments(ctx, filter)
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(fmt.Sprintf("# unset: %d\n", cnt))
		countUnset = cnt
	}
	{
		filter := bson.D{{"result.state", ResultStateDone}}
		cnt, err := d.plates().CountDocuments(ctx, filter)
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(fmt.Sprintf("# done: %d\n", cnt))
		countDone = cnt
	}
	{
		filter := bson.D{{"result.state", ResultStateError}}
		cnt, err := d.plates().CountDocuments(ctx, filter)
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(fmt.Sprintf("# error: %d\n", cnt))
		countError = cnt
	}

	debugInfo := &DebugInfo{
		CountUnset: countUnset,
		CountDone:  countDone,
		CountError: countError,
	}
	return buf.String(), debugInfo, nil
}

func (d *DB) CleanUp(ctx context.Context) error {
	filter := bson.D{{"plate.value", "0"}}
	res, err := d.plates().DeleteMany(ctx, filter)
	if err != nil {
		return err
	}

	log.Printf("CleanUp result: %+v", res)

	return nil
}

func (d *DB) GetWork(ctx context.Context, state string, num int) ([]string, bool, error) {
	filter := bson.D{{"result.state", ResultsStateUnset}}
	limit := int64(num)
	opts := &options.FindOptions{
		Limit: &limit,
	}
	res, err := d.plates().Find(ctx, filter, opts)
	if err != nil {
		return nil, false, err
	}
	if res.Err() != nil {
		if isNoDocs(res.Err()) {
			return nil, false, nil
		}
		return nil, false, res.Err()
	}

	var strs []string
	for res.Next(ctx) {
		var stored storedPlate
		if err := res.Decode(&stored); err != nil {
			return nil, false, err
		}
		strs = append(strs, stored.Plate.Value)
	}

	return strs, true, nil
}

func (d *DB) AddWork(ctx context.Context, plateValue, state, tag string) (bool, error) {
	return d.addWork(ctx, plateValue, state, tag)
}

func (d *DB) addWork(ctx context.Context, plateValue, state, tag string) (bool, error) {
	filter := bson.D{{"plate.value", plateValue}, {"plate.state", state}}
	res := d.plates().FindOne(ctx, filter)
	if res.Err() != nil {
		if !isNoDocs(res.Err()) {
			return false, res.Err()
		}
	} else {
		// exists already
		return true, nil
	}

	stored := storedPlate{
		Plate: plate{
			Value: plateValue,
			State: state,
		},
		Tag: tag,
		Result: storedResult{
			State: ResultsStateUnset,
		},
	}

	if _, err := d.plates().InsertOne(ctx, stored); err != nil {
		return false, err
	}

	return false, nil
}

type Add struct {
	Plate string
	State string
	Tag   string
}

// https://www.mongodb.com/developer/quickstart/golang-multi-document-acid-transactions/
func (d *DB) AddWorkManyNoExistingCheck(ctx context.Context, adds []Add) error {
	var storeds []interface{}
	for _, a := range adds {
		stored := storedPlate{
			Plate: plate{
				Value: a.Plate,
				State: a.State,
			},
			Tag: a.Tag,
			Result: storedResult{
				State: ResultsStateUnset,
			},
		}
		storeds = append(storeds, stored)
	}
	if _, err := d.plates().InsertMany(ctx, storeds); err != nil {
		return err
	}
	return nil
}

// https://www.mongodb.com/developer/quickstart/golang-multi-document-acid-transactions/
func (d *DB) AddWorkMany(ctx context.Context, adds []Add) error {
	session, err := d.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	if err = mongo.WithSession(ctx, session, func(sessionContext mongo.SessionContext) error {
		if err = session.StartTransaction(txnOpts); err != nil {
			return err
		}
		for _, a := range adds {
			if _, err := d.addWork(ctx, a.Plate, a.State, a.Tag); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *DB) Update(ctx context.Context, plateValue, state string, resultState ResultState, total float64, resultErr string) error {
	start := time.Now()
	err := d.update(ctx, plateValue, state, resultState, total, resultErr)
	if *printUpdateTiming {
		elapsed := time.Since(start)
		log.Printf("update timing: %v", elapsed)
	}
	return err
}

func (d *DB) updateOld(ctx context.Context, plateValue, state string, resultState ResultState, total float64, resultErr string) error {
	filter := bson.D{{"plate.value", plateValue}, {"plate.state", state}}
	result := storedResult{
		State:     resultState,
		Error:     resultErr,
		TotalOwed: total,
	}
	update := bson.D{
		{"$set", bson.D{
			{"result", result},
		}},
	}
	if _, err := d.plates().UpdateOne(ctx, filter, update); err != nil {
		return err
	}
	return nil
}

func (d *DB) update(ctx context.Context, plateValue, state string, resultState ResultState, total float64, resultErr string) error {
	filter := bson.D{{"plate.value", plateValue}, {"plate.state", state}}
	if _, err := d.plates().DeleteOne(ctx, filter); err != nil {
		return err
	}
	// res := d.plates().FindOneAndDelete(ctx, filter)
	// if res.Err() != nil {
	// 	return res.Err()
	// }
	// var existing storedPlate
	// if err := res.Decode(&existing); err != nil {
	// 	return err
	// }
	// TODO
	tag := "vanity" // existing.Tag

	result := storedResult{
		State:     resultState,
		Error:     resultErr,
		TotalOwed: total,
	}
	stored := storedPlate{
		Plate: plate{
			Value: plateValue,
			State: state,
		},
		Result: result,
		Tag:    tag,
	}
	if _, err := d.plates().InsertOne(ctx, stored); err != nil {
		return err
	}
	return nil
}

type Update struct {
	Plate       string
	State       string
	ResultState ResultState
	Total       float64
	Error       string
}

// https://www.mongodb.com/developer/quickstart/golang-multi-document-acid-transactions/
func (d *DB) UpdateMany(ctx context.Context, updates []Update) error {
	session, err := d.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	if err = mongo.WithSession(ctx, session, func(sessionContext mongo.SessionContext) error {
		if err = session.StartTransaction(txnOpts); err != nil {
			return err
		}
		for _, u := range updates {
			if err := d.update(ctx, u.Plate, u.State, u.ResultState, u.Total, u.Error); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *DB) FindDonePlatesForState(ctx context.Context, state string) (chan string, chan error, error) {
	filter := bson.D{{"plate.state", state}, {"result.state", ResultStateDone}}
	cur, err := d.plates().Find(ctx, filter)
	if err != nil {
		return nil, nil, errors.Errorf("finding all plates for state: %s: %v", state, err)
	}
	plates := make(chan string)
	errors := make(chan error)
	go func() {
		for cur.Next(ctx) {
			var el storedPlate
			if err := cur.Decode(&el); err != nil {
				errors <- err
				continue
			}
			plates <- el.Plate.Value
		}
		close(plates)
		close(errors)
	}()
	return plates, errors, nil
}
