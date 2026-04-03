package batch

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
)

func Average(fname string, out io.Writer) {
	fi, err := os.Stat(fname)
	if err != nil {
		panic(err)
	}

	size := fi.Size()

	// Chunk in 64 MiB blocks
	chunkSize := int64(64 << 20)
	numChunks := (size + chunkSize - 1) / chunkSize

	offsets := make([][]int64, numChunks)
	for i := range offsets {
		chunkStart := int64(i) * chunkSize
		chunkEnd := chunkStart + chunkSize
		offsets[i] = []int64{chunkStart, min(chunkEnd, size)}
	}

	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Adjust chunk boundaries to align with line breaks
	for i := range len(offsets) - 1 {
		chunk := offsets[i]
		buf := make([]byte, 1)
		chunkEnd := chunk[1]

		for {
			_, err := f.ReadAt(buf, chunkEnd-1)
			if err != nil {
				if err != io.EOF {
					panic(err)
				}
			}

			if buf[0] == '\n' {
				offsets[i][1] = chunkEnd
				if i+1 < len(offsets) {
					offsets[i+1][0] = chunkEnd
				}
				break
			}

			if err == io.EOF {
				break
			}

			chunkEnd++
		}
	}

	processing := make(chan []int64, numChunks)
	merging := make(chan map[string]*[4]int, numChunks)

	for _, offset := range offsets {
		processing <- offset
	}
	close(processing)

	numWorkers := runtime.NumCPU()
	for range numWorkers {
		go func(f *os.File) {
			stations := make(map[string]*[4]int)

			for chunk := range processing {
				b := make([]byte, 10<<20)
				var offset int64
				pending := make([]byte, 0, 256)
				var station string
				var valRaw float64
				var val10 int

				for offset < chunk[1]-chunk[0] {
					remaining := chunk[1] - chunk[0] - offset
					readSize := min(remaining, int64(len(b)))

					n, err := f.ReadAt(b[:readSize], chunk[0]+offset)
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
			}
			merging <- stations
		}(f)
	}

	totalStations := make(map[string]*[4]int)

	for range numWorkers {
		stations := <-merging
		for station, stats := range stations {
			if _, seen := totalStations[station]; !seen {
				totalStations[station] = &[4]int{stats[0], stats[1], stats[2], stats[3]}
			} else {
				totalStations[station][0] = min(totalStations[station][0], stats[0])
				totalStations[station][1] = max(totalStations[station][1], stats[1])
				totalStations[station][2] += stats[2]
				totalStations[station][3] += stats[3]
			}
		}
	}

	stationNames := slices.Collect(maps.Keys(totalStations))
	slices.Sort(stationNames)

	for _, name := range stationNames {
		stats := totalStations[name]
		fmt.Fprintf(out, "%s;%.1f;%.1f;%.1f\n", name, float64(stats[0])*0.1, mean1BRC(stats[2], stats[3]), float64(stats[1])*0.1)
	}
}

func mean1BRC(sumTenths int, count int) float64 {
	roundedTenths := math.Floor(float64(sumTenths)/float64(count) + 0.5)
	return roundedTenths / 10.0
}
