package addwork

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/fatih/color"
	"github.com/spudtrooper/goutil/check"
	goutillog "github.com/spudtrooper/goutil/log"
	"github.com/spudtrooper/nyc-parking-violations/db"
)

var (
	start              = flag.String("start", "", "start string")
	end                = flag.String("end", "", "end string")
	state              = flag.String("state", "NY", "plate state")
	platesFile         = flag.String("plates_file", "", "CVS containing one plate value per line")
	plateCSVFile       = flag.String("plates_csv_file", "", "CVS containing one plate value per line")
	plateCSVFileColumn = flag.Int("plates_csv_file_col", -1, "column index of license plate in CSV file")
	plateCSVSkipFirst  = flag.Bool("plates_csv_skip_first", false, "skip the first line in the CSV file")
	threads            = flag.Int("threads", 20, "number of threads")
	dryRun             = flag.Bool("dry_run", false, "just print what we would do")
)

var log = goutillog.MakeLog("add-work", goutillog.MakeLogColor(true))

type strRange struct {
	from, to rune
}

func makeStrRange(from, to rune) *strRange {
	check.Check(from <= to, check.CheckMessage("from must be < to"))
	return &strRange{
		from: from,
		to:   to,
	}
}

func (s *strRange) ToList() []rune {
	var res []rune
	for i := s.from; i <= s.to; i++ {
		if unicode.IsLetter(i) || unicode.IsDigit(i) {
			res = append(res, i)
		}
	}
	return res
}

func addToStrings(i int, rngs []*strRange, buf []rune, ch *chan string) {
	if i == len(rngs) {
		*ch <- string(buf)
		return
	}
	rng := rngs[i]
	strs := rng.ToList()
	for _, s := range strs {
		b := buf[:]
		b = append(b, s)
		addToStrings(i+1, rngs, b, ch)
	}
}

func addFromFile(ctx context.Context, d *db.DB, f string, colIndex int, skipFirst bool) {
	platesCh := make(chan string)
	go func() {
		in, err := os.Open(f)
		check.Err(err)
		defer in.Close()
		csvIn := csv.NewReader(in)
		first := true
		for {
			rec, err := csvIn.Read()
			if err == io.EOF {
				break
			}
			check.Err(err)
			if skipFirst && first {
				first = false
				continue
			}
			first = false
			platesCh <- rec[colIndex]
		}
		close(platesCh)
	}()

	var wg sync.WaitGroup
	var added, existed int64
	var done int32
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for plate := range platesCh {
				if *dryRun {
					log.Printf("add %s:%s", plate, *state)
					atomic.AddInt64(&added, 1)
				} else {
					exists, err := d.AddWork(ctx, plate, *state)
					check.Err(err)
					if exists {
						atomic.AddInt64(&existed, 1)
					} else {
						atomic.AddInt64(&added, 1)
					}
				}
			}
			atomic.AddInt32(&done, 1)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		var lastAdded, lastExisted int64
		for {
			time.Sleep(1 * time.Second)
			elapsed := time.Since(start)
			rate := float64(added+existed) / float64(elapsed.Milliseconds()/1000)
			diffAdded := added - lastAdded
			diffExisted := existed - lastExisted
			log.Printf("elapsed: %s | added: %5d (+%s) | existed: %5d (+%s) | rate: %0.1f/s",
				color.GreenString(fmt.Sprintf("%15v", elapsed)),
				added,
				color.HiGreenString(fmt.Sprintf("%5d", diffAdded)),
				existed,
				color.HiGreenString(fmt.Sprintf("%5d", diffExisted)),
				rate)
			lastAdded = added
			lastExisted = existed
			if done > 0 {
				break
			}
		}
	}()

	wg.Wait()
}

func createStrings() chan string {
	check.Check(*start != "", check.CheckMessage("--start required"))
	check.Check(*end != "", check.CheckMessage("--end required"))
	check.Check(len(*start) == len(*end), check.CheckMessage("len(--start) must equal len(--end)"))

	res := make(chan string)

	var rngs []*strRange
	froms, tos := []rune(*start), []rune(*end)
	for i := 0; i < len(froms); i++ {
		rng := makeStrRange(froms[i], tos[i])
		rngs = append(rngs, rng)
	}

	go func() {
		addToStrings(0, rngs, []rune{}, &res)
		close(res)
	}()

	return res
}

func addFromFlags(ctx context.Context, d *db.DB) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		strs := createStrings()
		for s := range strs {
			_, err := d.AddWork(ctx, s, *state)
			check.Err(err)
		}
	}()
	wg.Wait()
}

func Main(ctx context.Context) {
	d, err := db.MakeDB(ctx)
	check.Err(err)

	if *platesFile != "" {
		addFromFile(ctx, d, *platesFile, 0, false)
		return
	}

	if *plateCSVFile != "" {
		check.Check(*plateCSVFileColumn != -1)
		addFromFile(ctx, d, *plateCSVFile, *plateCSVFileColumn, *plateCSVSkipFirst)
		return
	}

	addFromFlags(ctx, d)
}
