package dowork

import (
	"context"
	"flag"
	"strings"
	"sync"

	"github.com/spudtrooper/goutil/check"
	goutillog "github.com/spudtrooper/goutil/log"
	"github.com/spudtrooper/goutil/must"
	"github.com/spudtrooper/goutil/slice"
	"github.com/spudtrooper/nyc-parking-violations/db"
	"github.com/spudtrooper/nyc-parking-violations/find"
)

var (
	threads    = flag.Int("threads", 20, "number of threads")
	workLimit  = flag.Int("work_limit", -1, "limit of work for each thread ")
	state      = flag.String("state", "NY", "plate state")
	plates     = flag.String("plates", "", "comma-delimited list of plates to look up")
	platesFile = flag.String("plates_file", "", "CVS containing one plate value per line")
)

var log = goutillog.MakeLog("plates", goutillog.MakeLogColor(true))

type workQueue struct {
	db  *db.DB
	buf []string
	cur int
	mu  sync.Mutex
}

func makeWorkQueue(db *db.DB) *workQueue {
	return &workQueue{
		db: db,
	}
}

func (w *workQueue) Next(ctx context.Context) (string, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) == 0 || w.cur >= len(w.buf) {
		strs, ok, err := w.db.GetWork(ctx, *state, 4**threads)
		if err != nil {
			return "", false, err
		}
		if !ok {
			return "", false, nil
		}
		w.buf = strs
		w.cur = 0
	}
	if len(w.buf) == 0 {
		return "", false, nil
	}
	res := w.buf[w.cur]
	w.cur++
	return res, true, nil
}

func processPlates(ctx context.Context, d *db.DB, plates []string) {
	platesCh := make(chan string)
	go func() {
		for _, plate := range plates {
			platesCh <- strings.TrimSpace(plate)
		}
		close(platesCh)
	}()
	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done := 0
			for plate := range platesCh {
				total, err := find.FindTotalOwed(plate, *state)
				if err != nil {
					if err := d.Update(ctx, plate, *state, db.ResultStateError, 0, err.Error()); err != nil {
						log.Printf("error: %v", err)
					}
					continue
				}
				log.Printf("%s -> $%0.2f", plate, total)
				if err := d.Update(ctx, plate, *state, db.ResultStateDone, total, ""); err != nil {
					log.Printf("error: %v", err)
					continue
				}
				done++
				if *workLimit != -1 && done >= *workLimit {
					break
				}
			}
		}()
	}
	wg.Wait()
}

func Main(ctx context.Context) {
	d, err := db.MakeDB(ctx)
	check.Err(err)

	if *plates != "" {
		processPlates(ctx, d, slice.Strings(*plates, ","))
		return
	}

	if *platesFile != "" {
		processPlates(ctx, d, must.ReadLines(*platesFile))
		return
	}

	var wg sync.WaitGroup
	q := makeWorkQueue(d)
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done := 0
			for {
				plate, ok, err := q.Next(ctx)
				if err != nil {
					log.Printf("error: %v", err)
					continue
				}
				if !ok {
					log.Printf("we're done")
					s, err := d.DebugString(ctx)
					check.Err(err)
					log.Println("\n" + s)
					break
				}
				total, err := find.FindTotalOwed(plate, *state)
				if err != nil {
					if err := d.Update(ctx, plate, *state, db.ResultStateError, 0, err.Error()); err != nil {
						log.Printf("error: %v", err)
					}
					continue
				}
				log.Printf("%s -> $%0.2f", plate, total)
				if err := d.Update(ctx, plate, *state, db.ResultStateDone, total, ""); err != nil {
					log.Printf("error: %v", err)
				}
				done++
				if *workLimit != -1 && done >= *workLimit {
					break
				}
			}
		}()
	}
	wg.Wait()
}
