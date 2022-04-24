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

type transactionAdder struct {
	d               *db.DB
	mu              sync.Mutex
	adds            []db.Add
	transactionSize int
}

func makeTransactionAdder(d *db.DB) *transactionAdder {
	return &transactionAdder{
		d:               d,
		transactionSize: *txSize,
	}
}

func (t *transactionAdder) Add(ctx context.Context, plate, state, tag string) {
	add := db.Add{
		Plate: plate,
		State: state,
		Tag:   tag,
	}
	t.mu.Lock()
	t.adds = append(t.adds, add)

	var adds []db.Add
	if len(t.adds) == t.transactionSize {
		adds = t.adds[:]
		t.adds = []db.Add{}
	}
	t.mu.Unlock()

	if len(adds) > 0 {
		t.flush(ctx, adds)
	}
}

func (t *transactionAdder) Flush(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flush(ctx, t.adds)
}

func (t *transactionAdder) flush(ctx context.Context, adds []db.Add) {
	if err := t.d.AddWorkMany(ctx, adds); err != nil {
		log.Printf("error: %v", err)
	}
}

func addFromFileBatched(ctx context.Context, d *db.DB, f string, colIndex int, skipFirst bool) {
	t := makeTransactionAdder(d)
	addFromFileGeneric(ctx, d, f, colIndex, skipFirst,
		func(ctx context.Context, plate, state, tag string, existed, added *int64) {
			t.Add(ctx, plate, state, tag)
			atomic.AddInt64(added, 1)
		},
		func(ctx context.Context) {
			t.Flush(ctx)
		},
	)
}

func addFromFile(ctx context.Context, d *db.DB, f string, colIndex int, skipFirst bool) {
	addFromFileGeneric(ctx, d, f, colIndex, skipFirst,
		func(ctx context.Context, plate, state, tag string, existed, added *int64) {
			exists, err := d.AddWork(ctx, plate, state, tag)
			check.Err(err)
			if exists {
				atomic.AddInt64(existed, 1)
			} else {
				atomic.AddInt64(added, 1)
			}
		},
		func(ctx context.Context) {
			// nothing
		},
	)
}

func addFromFileGeneric(ctx context.Context, d *db.DB, f string, colIndex int, skipFirst bool,
	add func(ctx context.Context, plate, state, tag string, existed, added *int64), flush func(ctx context.Context)) {
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
					add(ctx, plate, *state, *tag, &existed, &added)
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
				color.GreenString(fmt.Sprintf("%20v", elapsed)),
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

	flush(ctx)
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
		if *txSize > 0 {
			addFromFileBatched(ctx, d, *platesFile, 0, false)
		} else {
			addFromFile(ctx, d, *platesFile, 0, false)
		}
		return
	}

	if *plateCSVFile != "" {
		check.Check(*plateCSVFileColumn != -1)
		if *txSize > 0 {
			addFromFileBatched(ctx, d, *plateCSVFile, *plateCSVFileColumn, *plateCSVSkipFirst)
		} else {
			addFromFile(ctx, d, *plateCSVFile, *plateCSVFileColumn, *plateCSVSkipFirst)
		}
		return
	}

	addFromFlags(ctx, d)
}
