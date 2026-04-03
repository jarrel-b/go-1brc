package batch

import (
	"os"
	"testing"
)

func BenchmarkAverage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Average("../../measurements_trunc.txt", os.Stdout)
	}
}
