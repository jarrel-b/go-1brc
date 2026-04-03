package main

import (
	"go-1brc/src/customreader"
	"os"
)

func main() {
	customreader.Average("./measurements_sample.txt", os.Stdout)
}
