package main

import (
	"flag"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/spudtrooper/goutil/check"
	"github.com/spudtrooper/nyc-parking-violations/nycparkingviolations"
)

var (
	plate  = flag.String("plate", "", "Plate number")
	plates = flag.String("plates", "", "Comma-separated list of plate numbers")
	state  = flag.String("state", "NY", "State of the plate")

	// value="65.00" step="0.01"
	amountRE = regexp.MustCompile(`value="(\d+)\.(\d{2})" step="0.01"`)
)

func realMain() error {
	if *plate == "" && *plates == "" {
		return errors.Errorf("--plate or --plates required")
	}
	if *plate != "" {
		total, err := nycparkingviolations.FindTotalOwed(*plate, *state)
		if err != nil {
			return err
		}
		fmt.Printf("$%0.2f\n", total)
	} else {
		for _, plate := range strings.Split(*plates, ",") {
			plate = strings.TrimSpace(plate)
			total, err := nycparkingviolations.FindTotalOwed(plate, *state)
			if err != nil {
				return err
			}
			fmt.Printf("%s:$%0.2f\n", plate, total)
		}
	}

	return nil
}

func main() {
	flag.Parse()
	check.Err(realMain())
}
