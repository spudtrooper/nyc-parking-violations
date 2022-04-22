package main

import (
	"context"
	"flag"

	"github.com/spudtrooper/nyc-parking-violations/cleanup"
)

func main() {
	flag.Parse()
	cleanup.Main(context.Background())
}
