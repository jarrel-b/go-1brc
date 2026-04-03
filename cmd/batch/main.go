package main

import (
	"go-1brc/src/batch"
	"os"
)

func main() {
	batch.Average("./measurements_sample.txt", os.Stdout)
}
