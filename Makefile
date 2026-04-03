test-%: bin/% calculate_average_%.sh
	./test.sh $*
	go test ./src/$* -bench=BenchmarkAverage

bin/%:
	go build -o $@ cmd/$*/main.go

calculate_average_%.sh:
	echo '#!/bin/bash\n./bin/$*' > $@ && chmod +x $@
