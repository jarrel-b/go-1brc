package batchopt

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"unsafe"
)

type stats [4]int

type stationHeap []string

func (h stationHeap) Len() int           { return len(h) }
func (h stationHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h stationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *stationHeap) Push(x any) {
	*h = append(*h, x.(string))
}

func (h *stationHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

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
	merging := make(chan map[string]*stats, numChunks)

	for _, offset := range offsets {
		processing <- offset
	}
	close(processing)

	numWorkers := runtime.NumCPU()
	for range numWorkers {
		go func(f *os.File) {
			stations := make(map[string]*stats)
			work := processing
			var done chan map[string]*stats

			for {
				select {
				case chunk, ok := <-work:
					if !ok {
						work = nil
						done = merging
						continue
					}

					b := make([]byte, 10<<20)
					var offset int64
					pending := make([]byte, 0, 256)
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

							// Avoid string allocation
							key := unsafe.String(unsafe.SliceData(line[:delimIdx]), delimIdx)
							sts := stations[key]
							if sts == nil {
								station := string(line[:delimIdx])
								stations[station] = &stats{val10, val10, val10, 1}
							} else {
								sts[0] = min(sts[0], val10)
								sts[1] = max(sts[1], val10)
								sts[2] += val10
								sts[3]++
							}

							pending = pending[newLineIdx+1:]
						}

						offset += int64(n)

						if err == io.EOF {
							break
						}
					}

				case done <- stations:
					return
				}
			}
		}(f)
	}

	totalStations := make(map[string]*stats)
	stationNames := make(stationHeap, 0)

	for range numWorkers {
		stations := <-merging
		for station, s := range stations {
			sts := totalStations[station]
			if sts == nil {
				totalStations[station] = &stats{s[0], s[1], s[2], s[3]}
				heap.Push(&stationNames, station)
			} else {
				sts[0] = min(sts[0], s[0])
				sts[1] = max(sts[1], s[1])
				sts[2] += s[2]
				sts[3] += s[3]
			}
		}
	}

	for stationNames.Len() > 0 {
		name := heap.Pop(&stationNames).(string)
		stats := totalStations[name]
		fmt.Fprintf(out, "%s;%.1f;%.1f;%.1f\n", name, float64(stats[0])*0.1, mean1BRC(stats[2], stats[3]), float64(stats[1])*0.1)
	}
}

func mean1BRC(sumTenths int, count int) float64 {
	roundedTenths := math.Floor(float64(sumTenths)/float64(count) + 0.5)
	return roundedTenths / 10.0
}
