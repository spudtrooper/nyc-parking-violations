package dowork

import (
	"context"
	"flag"
	"sync"

	"github.com/spudtrooper/goutil/check"
	goutillog "github.com/spudtrooper/goutil/log"
	"github.com/spudtrooper/nyc-parking-violations/db"
	"github.com/spudtrooper/nyc-parking-violations/find"
)

var (
	threads   = flag.Int("threads", 20, "number of threads")
	workLimit = flag.Int("work_limit", -1, "limit of work for each thread ")
	state     = flag.String("state", "NY", "plate state")
)

var log = goutillog.MakeLog("", goutillog.MakeLogColor(true))

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
		log.Printf("refilling...")
		strs, ok, err := w.db.GetWork(ctx, *state, *threads)
		if err != nil {
			return "", false, err
		}
		if !ok {
			return "", false, nil
		}
		w.buf = strs
		w.cur = 0
	}
	res := w.buf[w.cur]
	w.cur++
	return res, true, nil
}

func Main(ctx context.Context) {
	d, err := db.MakeDB(ctx)
	log.Printf("db: %v", d)
	check.Err(err)

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
