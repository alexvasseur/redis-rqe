# redis-rqe
Redis JSON with Redis Query Engine - fake data &amp; simple performance testing

# Setup

```
go mod tidy
go mod download
```

# Notes

By default each subroutine opens its own connection
Defaults to 20 subroutine and 2M entries which amounts for 1.22GB of dataset (700 byte on average)

# Loading data (insert/update)

This will display overall stats (multiplies by the pipeline size to reflect ops/s, default 100)
```
go run refaker.go -h redis-12000.cluster.avasseur-default.demo.redislabs.com -p 12000 -a xxx

[Metrics] Count: 7593  Mean: 10.98ms  95th: 23.41ms  99th: 59.09ms  Rate: 151860.00/s
```

# Running query load

This will query as `FT.SEARCH person_index @country:{France} @age:[45 50] SORTBY age LIMIT 0 10` and display overall stats.

The query is configured to fetch data (not using FT.AGGREGATE, and using `NoContent: false`).
```
go run requery/requery.go -h redis-12000.cluster.avasseur-default.demo.redislabs.com -p 12000 -a xxx

[Metrics] Count: 3871  Mean: 25.92ms  95th: 35.75ms  99th: 40.24ms  Rate: 769.60/s
```

or run both to get a mixed workload
```
[Metrics] Count: 3714  Mean: 24.65ms  95th: 39.69ms  99th: 79.22ms  Rate: 74280.00/s
[Metrics] Count: 214  Mean: 413.34ms  95th: 1086.29ms  99th: 1175.54ms  Rate: 42.80/s
[Metrics] Count: 3685  Mean: 25.35ms  95th: 38.60ms  99th: 73.32ms  Rate: 73700.00/s
[Metrics] Count: 335  Mean: 333.38ms  95th: 1113.37ms  99th: 1258.99ms  Rate: 66.80/s
[Metrics] Count: 4166  Mean: 21.69ms  95th: 30.36ms  99th: 36.41ms  Rate: 83320.00/s
```

# TODO

- rate limit the insert/update
- provide other query args
- provide hashtag on country