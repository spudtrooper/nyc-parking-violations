package addwork

import (
	"context"
	"flag"
	"log"
	"sync"
	"unicode"

	"github.com/spudtrooper/goutil/check"
	"github.com/spudtrooper/nyc-parking-violations/db"
)

var (
	start = flag.String("start", "", "start string")
	end   = flag.String("end", "", "end string")
	state = flag.String("state", "NY", "plate state")
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
	db, err := db.MakeDB(ctx)
	log.Printf("db: %v", db)
	check.Err(err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		strs := createStrings()
		for s := range strs {
			err := db.AddWork(ctx, s, *state)
			check.Err(err)
		}
	}()
	wg.Wait()
}
