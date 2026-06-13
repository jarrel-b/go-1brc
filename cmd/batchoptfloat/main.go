package main

import (
	"go-1brc/src/batchoptfloat"
	"os"
)

func main() {
	batchoptfloat.Average("./measurements_sample.txt", os.Stdout)
}
