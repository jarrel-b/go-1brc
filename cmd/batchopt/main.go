package main

import (
	"go-1brc/src/batchopt"
	"os"
)

func main() {
	batchopt.Average("./measurements_sample.txt", os.Stdout)
}
