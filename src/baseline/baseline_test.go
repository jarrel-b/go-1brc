package baseline

import (
	"io"
	"testing"
)

func BenchmarkAverage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Average("../../measurements.txt", io.Discard)
	}
}
