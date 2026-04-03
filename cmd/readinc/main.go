package main

import (
	"go-1brc/src/readinc"
	"os"
)

func main() {
	readinc.Average("./measurements_sample.txt", os.Stdout)
}
