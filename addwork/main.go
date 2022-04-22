package addwork

import (
	"context"
	"flag"
	"strings"
	"sync"
	"unicode"

	"github.com/spudtrooper/goutil/check"
	"github.com/spudtrooper/goutil/must"
	"github.com/spudtrooper/nyc-parking-violations/db"
)

var (
	start      = flag.String("start", "", "start string")
	end        = flag.String("end", "", "end string")
	state      = flag.String("state", "NY", "plate state")
	platesFile = flag.String("plates_file", "", "CVS containing one plate value per line")
	threads    = flag.Int("threads", 20, "number of threads")
)

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

func Main(ctx context.Context) {
	d, err := db.MakeDB(ctx)
	check.Err(err)

	if *platesFile != "" {
		platesCh := make(chan string)
		go func() {
			for _, plate := range must.ReadLines(*platesFile) {
				platesCh <- strings.TrimSpace(plate)
			}
			close(platesCh)
		}()
		var wg sync.WaitGroup
		for i := 0; i < *threads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for plate := range platesCh {
					check.Err(d.AddWork(ctx, plate, *state))
				}
			}()
		}
		wg.Wait()
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		strs := createStrings()
		for s := range strs {
			err := d.AddWork(ctx, s, *state)
			check.Err(err)
		}
	}()
	wg.Wait()
}
