package main

import (
	"go-1brc/src/baseline"
	"os"
)

func main() {
	baseline.Average("./measurements_sample.txt", os.Stdout)
}
