package common

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/fatih/color"
	"github.com/spudtrooper/nyc-parking-violations/db"
)

var (
	red   = color.New(color.FgRed)
	white = color.New(color.FgWhite)
	green = color.New(color.FgGreen)
)

func MonitorDBInLoop(ctx context.Context, d *db.DB) {
	var debugInfo db.DebugInfo
	start := time.Now()
	for {
		_, nextDebugInfo, err := d.DebugString(ctx)
		if err != nil {
			log.Printf("err: %v", err)
		} else {
			vals := func(next, cur int64) (int64, string, *color.Color) {
				diff := next - cur
				if cur == 0 {
					return 0, " ", white
				}
				if diff > 0 {
					return diff, "+", green
				}
				if diff < 0 {
					return diff, "-", red
				}
				return diff, " ", white
			}
			elapsed := time.Since(start)
			doneDiff, doneSign, doneDiffColor := vals(nextDebugInfo.CountDone, debugInfo.CountDone)
			unsetDiff, unsetSign, unsetDiffColor := vals(nextDebugInfo.CountUnset, debugInfo.CountUnset)
			errorDiff, errorSign, errorDiffColor := vals(nextDebugInfo.CountError, debugInfo.CountError)
			log.Printf("[elapsed: %s] done: %s (%s%s) | unset: %s (%s%s) | error: %s (%s%s)",
				color.YellowString(fmt.Sprintf("%20s", elapsed)),
				color.CyanString(fmt.Sprintf("%9d", nextDebugInfo.CountDone)),
				doneSign,
				doneDiffColor.Sprintf("%9d", int64(math.Abs(float64(doneDiff)))),
				color.CyanString(fmt.Sprintf("%9d", nextDebugInfo.CountUnset)),
				unsetSign,
				unsetDiffColor.Sprintf("%9d", int64(math.Abs(float64(unsetDiff)))),
				color.CyanString(fmt.Sprintf("%9d", nextDebugInfo.CountError)),
				errorSign,
				errorDiffColor.Sprintf("%9d", int64(math.Abs(float64(errorDiff)))),
			)
			debugInfo = *nextDebugInfo
		}
		time.Sleep(10 * time.Second)
	}
}
