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
	"github.com/spudtrooper/nyc-parking-violations/common"
	"github.com/spudtrooper/nyc-parking-violations/db"
	"github.com/spudtrooper/nyc-parking-violations/find"
)

var (
	threads    = flag.Int("threads", 20, "number of threads")
	workLimit  = flag.Int("work_limit", -1, "limit of work for each thread ")
	state      = flag.String("state", "NY", "plate state")
	plates     = flag.String("plates", "", "comma-delimited list of plates to look up")
	platesFile = flag.String("plates_file", "", "CVS containing one plate value per line")
	txSize     = flag.Int("tx_size", 0, "# of updates per transaction, if zero we don't use the batch updater")
	verbose    = flag.Bool("verbose", false, "verbose logging")
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
		i := i
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
				log.Printf("thread #%3d: %s -> $%0.2f", i, plate, total)
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

type transactionUpdater struct {
	d               *db.DB
	mu              sync.Mutex
	updates         []db.Update
	transactionSize int
}

func makeTransactionUpdater(d *db.DB) *transactionUpdater {
	return &transactionUpdater{
		d:               d,
		transactionSize: *txSize,
	}
}

func (t *transactionUpdater) Add(ctx context.Context, plate, state string, resultState db.ResultState, total float64, err error) {
	var e string
	if err != nil {
		e = err.Error()
	}
	u := db.Update{
		Plate:       plate,
		State:       state,
		ResultState: resultState,
		Total:       total,
		Error:       e,
	}
	t.mu.Lock()
	t.updates = append(t.updates, u)

	var updates []db.Update
	if len(t.updates) == t.transactionSize {
		updates = t.updates[:]
		t.updates = []db.Update{}
	}
	t.mu.Unlock()

	if len(updates) > 0 {
		t.flush(ctx, updates)
	}
}

func (t *transactionUpdater) Flush(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flush(ctx, t.updates)
}

func (t *transactionUpdater) flush(ctx context.Context, updates []db.Update) {
	if err := t.d.UpdateMany(ctx, updates); err != nil {
		log.Printf("error: %v", err)
	}
}

func processPlatesFromDBWithTransactions(ctx context.Context, d *db.DB) {
	var wg sync.WaitGroup
	q := makeWorkQueue(d)
	u := makeTransactionUpdater(d)
	for i := 0; i < *threads; i++ {
		i := i
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
					log.Printf("thread #%d done", i)
					break
				}
				total, err := find.FindTotalOwed(plate, *state)
				if err != nil {
					log.Printf("thread #%3d: %s -> $%0.2f error: %v", i, plate, total, err)
					u.Add(ctx, plate, *state, db.ResultStateError, 0, err)
					continue
				}
				log.Printf("thread #%3d: %s -> $%0.2f", i, plate, total)
				u.Add(ctx, plate, *state, db.ResultStateDone, total, nil)
				done++
				if *workLimit != -1 && done >= *workLimit {
					break
				}
			}
		}()
	}
	wg.Wait()
	u.Flush(ctx)
}

func processPlatesFromDB(ctx context.Context, d *db.DB) {
	var wg sync.WaitGroup
	q := makeWorkQueue(d)
	for i := 0; i < *threads; i++ {
		i := i
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
					log.Printf("thread #%d done", i)
					break
				}
				total, err := find.FindTotalOwed(plate, *state)
				if err != nil {
					if *verbose {
						log.Printf("thread #%3d: %s -> $%0.2f error: %v", i, plate, total, err)
					}
					go func() {
						if err := d.Update(ctx, plate, *state, db.ResultStateError, 0, err.Error()); err != nil {
							log.Printf("update error: %v", err)
						}
					}()
					continue
				}
				if *verbose {
					log.Printf("thread #%3d: %s -> $%0.2f", i, plate, total)
				}
				go func() {
					if err := d.Update(ctx, plate, *state, db.ResultStateDone, total, ""); err != nil {
						log.Printf("update error: %v", err)
					}
				}()
				done++
				if *workLimit != -1 && done >= *workLimit {
					break
				}
			}
		}()
	}

	go func() {
		common.MonitorDBInLoop(ctx, d)
	}()

	wg.Wait()
}

func Main(ctx context.Context) {
	d, err := db.MakeFromFlags(ctx)
	check.Err(err)

	if *plates != "" {
		processPlates(ctx, d, slice.Strings(*plates, ","))
		return
	}

	if *platesFile != "" {
		processPlates(ctx, d, must.ReadLines(*platesFile))
		return
	}

	if *txSize > 0 {
		processPlatesFromDBWithTransactions(ctx, d)
	} else {
		processPlatesFromDB(ctx, d)
	}
}
