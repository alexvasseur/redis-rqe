# redis-rqe
Redis JSON with Redis Query Engine - fake data &amp; simple performance testing

# Setup

go mod download

# Notes

By default each subroutine opens its own connection
Defaults to 20 subroutine and 2M entries which amounts for 1.22GB of dataset (700 byte on average)

# Loading data (insert/update)

This will display overall stats for the pipeline call (default 100)
```
go run refaker.go

[Metrics] Count: 7593  Mean: 10.98ms  95th: 23.41ms  99th: 59.09ms  Rate: 151860.00/s
```