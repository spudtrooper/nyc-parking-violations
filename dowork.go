package main

import (
	"context"
	"flag"

	"github.com/spudtrooper/nyc-parking-violations/dowork"
)

func main() {
	flag.Parse()
	dowork.Main(context.Background())
}
