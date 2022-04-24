package db

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/spudtrooper/goutil/or"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	dbPort = flag.Int("db_port", 0, "database port")
	dbName = flag.String("db_name", "", "database name")
)

type DB struct {
	dbName string
	client *mongo.Client
}

func MakeFromFlags(ctx context.Context) (*DB, error) {
	return Make(ctx, MakeDBPort(*dbPort), MakeDBDbName(*dbName))
}

func Make(ctx context.Context, mOpts ...MakeDBOption) (*DB, error) {
	opts := MakeMakeDBOptions(mOpts...)

	port := or.Int(opts.Port(), 27017)
	dbName := or.String(opts.DbName(), "nycparkingviolations")
	uri := fmt.Sprintf("mongodb://localhost:%d", port)
	log.Printf("trying to connect to %s to create %s", uri, dbName)
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	log.Printf("connected to %s", dbName)

	db := client.Database(dbName)
	db.Collection("plates")

	res := &DB{
		dbName: dbName,
		client: client,
	}
	return res, nil
}

func (d *DB) database() *mongo.Database {
	return d.client.Database(d.dbName)
}

func (d *DB) collection(name string) *mongo.Collection {
	return d.database().Collection(name)
}

func (d *DB) Disconnect(ctx context.Context) error {
	return d.client.Disconnect(ctx)
}
