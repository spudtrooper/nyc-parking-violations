package main

import (
	"context"
	"flag"

	"github.com/spudtrooper/nyc-parking-violations/addwork"
)

func main() {
	flag.Parse()
	addwork.Main(context.Background())
}
