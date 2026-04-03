package readinc

import (
	"encoding/csv"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
)

type stats struct {
	min int
	max int
	sum int
	cnt int
}

func Average(fname string, out io.Writer) {
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	stations := make(map[string]stats, 10_000)

	r := csv.NewReader(f)
	r.Comma = ';'

	total := 0
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		total++

		if _, seen := stations[record[0]]; !seen {
			stations[record[0]] = stats{min: 1e9, max: -1e9}
		}

		val, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			panic(err)
		}
		val10 := int(val * 10)

		st := stations[record[0]]
		st.min = min(st.min, val10)
		st.max = max(st.max, val10)
		st.sum += val10
		st.cnt++
		stations[record[0]] = st
	}

	stationNames := slices.Collect(maps.Keys(stations))
	slices.Sort(stationNames)

	for _, name := range stationNames {
		stats := stations[name]
		fmt.Fprintf(out, "%s;%.1f;%.1f;%.1f\n", name, float64(stats.min)*0.1, mean1BRC(stats.sum, stats.cnt), float64(stats.max)*0.1)
	}
}

func mean1BRC(sumTenths int, count int) float64 {
	roundedTenths := math.Floor(float64(sumTenths)/float64(count) + 0.5)
	return roundedTenths / 10.0
}
