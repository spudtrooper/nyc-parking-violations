package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spudtrooper/goutil/check"
	"github.com/spudtrooper/nyc-parking-violations/nycparkingviolations"
)

var (
	plate      = flag.String("plate", "", "Plate number")
	plates     = flag.String("plates", "", "Comma-separated list of plate numbers")
	platesFile = flag.String("plates_file", "", "File containing one plate per line")
	state      = flag.String("state", "NY", "State of the plate")
)

func realMain() error {
	if *plate == "" && *plates == "" && *platesFile == "" {
		return errors.Errorf("--plate or --plates or --plates_file required")
	}
	if *plate != "" {
		total, err := nycparkingviolations.FindTotalOwed(*plate, *state)
		if err != nil {
			return err
		}
		fmt.Printf("$%0.2f\n", total)
	} else if *platesFile != "" {
		plates := make(chan string)
		results := make(chan nycparkingviolations.Result)
		errs := make(chan error)

		f, err := os.Open(*platesFile)
		if err != nil {
			return err
		}
		defer f.Close()

		go func() {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				if plate := scanner.Text(); plate != "" {
					plates <- plate
				}
			}
			close(plates)
		}()

		go func() {
			nycparkingviolations.FindTotalOwedBatch(*state, plates, results, errs)
			close(results)
			close(errs)
		}()

		for r := range results {
			fmt.Printf("%s:$%0.2f\n", r.Plate, r.Total)
		}
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
