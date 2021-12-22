package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"

	"github.com/spudtrooper/goutil/check"
)

var (
	plate = flag.String("plate", "", "Plate number")
	state = flag.String("state", "NY", "State of the plate")

	// value="65.00" step="0.01"
	amountRE = regexp.MustCompile(`value="(\d+)\.(\d{2})" step="0.01"`)
)

func realMain() error {
	var body = []byte(fmt.Sprintf(`PLATE_NUMBER=%s&PLATE_STATE=%s&PLATE_TYPE=++`, *plate, *state))
	req, err := http.NewRequest("POST",
		"https://a836-citypay.nyc.gov/citypay/Parking/searchResults", bytes.NewBuffer(body))
	if err != nil {
		return err
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
		return err
	}
	defer resp.Body.Close()

	b, _ := ioutil.ReadAll(resp.Body)
	respBody := string(b)

	total := 0.0
	for _, m := range amountRE.FindAllStringSubmatch(respBody, -1) {
		dollars, err := strconv.Atoi(m[1])
		if err != nil {
			return err
		}
		cents, err := strconv.Atoi(m[2])
		if err != nil {
			return err
		}
		cost := float64(dollars) + float64(cents)/100.0
		total += cost
	}
	fmt.Printf("$%0.2f\n", total)

	return nil
}

func main() {
	flag.Parse()
	check.Err(realMain())
}
