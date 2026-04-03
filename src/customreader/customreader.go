package customreader

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
)

var stations = make(map[string]*[4]int, 10_000)

func Average(fname string, out io.Writer) {
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	b := make([]byte, 10<<20)
	var offset int64
	pending := make([]byte, 0, 20<<20)
	var station string
	var valRaw float64
	var val10 int

	for {
		n, err := f.ReadAt(b, offset)
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
		}

		if n > 0 {
			pending = append(pending, b[:n]...)
		}

		for {
			newLineIdx := bytes.IndexByte(pending, '\n')
			if newLineIdx == -1 {
				break
			}

			line := pending[:newLineIdx]
			delimIdx := bytes.IndexByte(line, ';')
			if delimIdx == -1 {
				panic("malformed record: missing semicolon")
			}

			valRaw, err = strconv.ParseFloat(string(line[delimIdx+1:]), 64)
			if err != nil {
				panic(err)
			}

			val10 = int(valRaw * 10)

			station = string(line[:delimIdx])
			if _, seen := stations[station]; !seen {
				stations[station] = &[4]int{val10, val10, val10, 1}
			} else {
				stations[station][0] = min(stations[station][0], val10)
				stations[station][1] = max(stations[station][1], val10)
				stations[station][2] += val10
				stations[station][3]++
			}

			pending = pending[newLineIdx+1:]
		}

		offset += int64(n)

		if err == io.EOF {
			break
		}
	}

	stationNames := slices.Collect(maps.Keys(stations))
	slices.Sort(stationNames)

	for _, name := range stationNames {
		stats := stations[name]
		fmt.Fprintf(out, "%s;%.1f;%.1f;%.1f\n", name, float64(stats[0])*0.1, mean1BRC(stats[2], stats[3]), float64(stats[1])*0.1)
	}
}

func mean1BRC(sumTenths int, count int) float64 {
	roundedTenths := math.Floor(float64(sumTenths)/float64(count) + 0.5)
	return roundedTenths / 10.0
}
