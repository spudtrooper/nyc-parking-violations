package nycparkingviolations

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"sync"
)

var (
	// value="65.00" step="0.01"
	amountRE = regexp.MustCompile(`value="(\d+)\.(\d{2})" step="0.01"`)
)

type Result struct {
	Plate string
	Total float64
}

func FindTotalOwedBatch(state string, in chan string, out chan Result, errs chan error) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for plate := range in {
				total, err := findTotalOwed(plate, state)
				if err != nil {
					errs <- err
				} else {
					out <- Result{Plate: plate, Total: total}
				}
			}
		}()
	}
	wg.Wait()
}

func FindTotalOwed(plate, state string) (float64, error) {
	return findTotalOwed(plate, state)
}

func findTotalOwed(plate, state string) (float64, error) {
	if state == "" {
		state = "NY"
	}
	var body = []byte(fmt.Sprintf(`PLATE_NUMBER=%s&PLATE_STATE=%s&PLATE_TYPE=++`, plate, state))
	req, err := http.NewRequest("POST",
		"https://a836-citypay.nyc.gov/citypay/Parking/searchResults", bytes.NewBuffer(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Set("accept-language", "en-US,en;q=0.9")
	req.Header.Set("cache-control", "max-age=0")
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("sec-ch-ua", "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"96\", \"Google Chrome\";v=\"96\"")
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", "\"macOS\"")
	req.Header.Set("sec-fetch-dest", "document")
	req.Header.Set("sec-fetch-mode", "navigate")
	req.Header.Set("sec-fetch-site", "same-origin")
	req.Header.Set("sec-fetch-user", "?1")
	req.Header.Set("upgrade-insecure-requests", "1")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	b, _ := ioutil.ReadAll(resp.Body)
	respBody := string(b)

	total := 0.0
	for _, m := range amountRE.FindAllStringSubmatch(respBody, -1) {
		dollars, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, err
		}
		cents, err := strconv.Atoi(m[2])
		if err != nil {
			return 0, err
		}
		cost := float64(dollars) + float64(cents)/100.0
		total += cost
	}

	return total, nil
}
