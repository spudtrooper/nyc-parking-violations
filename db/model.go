package db

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type resultState string

const (
	ResultsStateUnset resultState = "unset"
	ResultStateError  resultState = "error"
	ResultStateDone   resultState = "done"
)

type plate struct {
	Value string
	State string
}

type storedResult struct {
	State     resultState
	Error     string
	TotalOwed float64
}

type storedPlate struct {
	Plate  plate
	Result storedResult
}

func isNoDocs(err error) bool {
	return strings.Contains(err.Error(), "no documents in result")
}

func (d *DB) DebugString(ctx context.Context) (string, error) {
	var buf bytes.Buffer

	{
		filter := bson.D{{"result.state", ResultsStateUnset}}
		cnt, err := d.collection("plates").CountDocuments(ctx, filter)
		if err != nil {
			return "", err
		}
		buf.WriteString(fmt.Sprintf("# unset: %d\n", cnt))
	}
	{
		filter := bson.D{{"result.state", ResultStateDone}}
		cnt, err := d.collection("plates").CountDocuments(ctx, filter)
		if err != nil {
			return "", err
		}
		buf.WriteString(fmt.Sprintf("# done: %d\n", cnt))
	}
	{
		filter := bson.D{{"result.state", ResultStateError}}
		cnt, err := d.collection("plates").CountDocuments(ctx, filter)
		if err != nil {
			return "", err
		}
		buf.WriteString(fmt.Sprintf("# error: %d\n", cnt))
	}

	return buf.String(), nil
}

func (d *DB) CleanUp(ctx context.Context) error {
	filter := bson.D{{"plate.value", "0"}}
	res, err := d.collection("plates").DeleteMany(ctx, filter)
	if err != nil {
		return err
	}

	log.Printf("CleanUp result: %+v", res)

	return nil
}

func (d *DB) AddWork(ctx context.Context, plateValue, state string) error {
	filter := bson.D{{"plate.value", plateValue}, {"plate.state", state}}
	res := d.collection("plates").FindOne(ctx, filter)
	if res.Err() != nil {
		if !isNoDocs(res.Err()) {
			return res.Err()
		}
	}

	stored := storedPlate{
		Plate: plate{
			Value: plateValue,
			State: state,
		},
		Result: storedResult{
			State: ResultsStateUnset,
		},
	}

	if _, err := d.collection("plates").InsertOne(ctx, stored); err != nil {
		return err
	}

	return nil
}

func (d *DB) GetWork(ctx context.Context, state string, num int) ([]string, bool, error) {
	filter := bson.D{{"result.state", ResultsStateUnset}}
	limit := int64(num)
	opts := &options.FindOptions{
		Limit: &limit,
	}
	res, err := d.collection("plates").Find(ctx, filter, opts)
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

func (d *DB) Update(ctx context.Context, plateValue, state string, resultState resultState, total float64, resultErr string) error {
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
	if _, err := d.collection("plates").UpdateOne(ctx, filter, update); err != nil {
		return err
	}
	return nil
}
