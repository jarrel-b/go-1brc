# go-1brc
Trying out the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc) using gov1.26.

Specs: M2 8 Core Macbook with 8GB RAM

|fork|description|time|
|-|-|-|
|baseline|Read everything into memory using standard library CSV reader|>11min|
|readinc|Read line by line, still using standard library CSV reader|~280s|
|customreader|Use custom parser to read records to avoid extra allocations, processing sequentially|~280s|
|batch|`customreader`, but batches in chunks of 64MiB, processing in parallel with 8 workers|~60s|
|batchopt|`batch` with a extra optimizations: reduce map lookups and string allocations, use heap for sorting stations, reduce channel send contention|~15s|
