package cleanup

import (
	"context"

	"github.com/spudtrooper/goutil/check"
	"github.com/spudtrooper/nyc-parking-violations/db"
)

func Main(ctx context.Context) {
	d, err := db.MakeDB(ctx)
	check.Err(err)
	check.Err(d.CleanUp(ctx))
}
