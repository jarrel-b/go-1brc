package batchopt

import (
	"os"
	"testing"
)

func BenchmarkAverage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Average("../../measurements.txt", os.Stdout)
	}
}
