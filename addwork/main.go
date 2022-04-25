package addwork

import (
	"context"
	"encoding/csv"
	"flag"
	"io"
	"os"
	"sync"
	"unicode"

	"github.com/spudtrooper/goutil/check"
	goutillog "github.com/spudtrooper/goutil/log"
	"github.com/spudtrooper/nyc-parking-violations/common"
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
	tag                = flag.String("tag", "", "extra tag to add to the entry")
	txSize             = flag.Int("tx_size", 0, "# of updates per transaction, if zero we don't use the batch adder")
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

func makePlatesChannel(f string, skipFirst bool, colIndex int, existing map[string]bool) (chan string, chan error) {
	platesCh := make(chan string)
	errs := make(chan error)
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
			if err != nil {
				errs <- err
				continue
			}
			if skipFirst && first {
				first = false
				continue
			}
			first = false
			plate := rec[colIndex]
			if !existing[plate] {
				platesCh <- rec[colIndex]
			}
		}
		close(platesCh)
		close(errs)
	}()
	return platesCh, errs
}

func addFromFile(ctx context.Context, d *db.DB, f string, colIndex int, skipFirst bool) {
	existingCh, errs, err := d.FindDonePlatesForState(ctx, *state)
	check.Err(err)
	existing := map[string]bool{}
	for p := range existingCh {
		existing[p] = true
	}
	for e := range errs {
		log.Printf("error: %v", e)
	}

	log.Printf("have %d existing done plates", len(existing))

	platesCh, _ := makePlatesChannel(f, skipFirst, colIndex, existing)
	var adds []db.Add
	for p := range platesCh {
		add := db.Add{
			Plate: p,
			State: *state,
			Tag:   *tag,
		}
		adds = append(adds, add)
	}
	log.Printf("adding %d new plates", len(adds))

	go func() {
		common.MonitorDBInLoop(ctx, d)
	}()

	if err := d.AddWorkManyNoExistingCheck(ctx, adds); err != nil {
		log.Printf("error: %v", err)
	}
	dbg, _ := d.MustDebugString(ctx)
	log.Printf("done: %s", dbg)
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
			_, err := d.AddWork(ctx, s, *state, *tag)
			check.Err(err)
		}
	}()
	wg.Wait()
}

func Main(ctx context.Context) {
	d, err := db.MakeFromFlags(ctx)
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
