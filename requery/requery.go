package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	//	"github.com/RediSearch/redisearch-go"
	"github.com/rcrowley/go-metrics"
	"github.com/redis/go-redis/v9"
)

var (
	rs       []*redis.Client
	timer    = metrics.NewTimer()
	registry = metrics.NewRegistry()
	done     = make(chan struct{})
	ctx      = context.Background()
)

// Function to create an index in RedisSearch
func index(host string, port int, password string) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,
		DB:       0,
	})

	// Index definition
	idx_country := &redis.FieldSchema{FieldName: "$.country", FieldType: redis.SearchFieldTypeTag, As: "country"}
	idx_age := &redis.FieldSchema{FieldName: "$.age", FieldType: redis.SearchFieldTypeNumeric, As: "age"}
	idx_firstname := &redis.FieldSchema{FieldName: "$.firstname", FieldType: redis.SearchFieldTypeText, As: "firstname"}
	_, err := client.FTCreate(ctx, "person_index",
		&redis.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"person:"}}, idx_country, idx_age, idx_firstname).Result()
	if err != nil {
		fmt.Println("Error creating index:", err)
		return
	}

	fmt.Println("Index created successfully.")
}

// Function to perform RedisSearch queries
func queryRedis(ctx context.Context, client *redis.Client, threadID int, count int, wg *sync.WaitGroup) {
	agemin := threadID * 5
	agemax := agemin + 5
	defer wg.Done()

	for i := 0; i < count; i++ {
		start := time.Now()
		result := client.FTSearchWithArgs(ctx, "person_index",
			fmt.Sprintf("@country:{France} @age:[%d %d]", agemin, agemax),
			&redis.FTSearchOptions{
				NoContent: false,
				SortBy:    []redis.FTSearchSortBy{{FieldName: "age", Asc: false}},
				Limit:     10,
			})
		var out = result.Val().Total
		if out < 0 {
			log.Fatal("never happens")
		}
		//for _, doc := range result.Val().Docs {
		//fmt.Println(doc.Fields["$.firstname"])
		//}
		timer.UpdateSince(start)
	}

	// agg := redisearch.NewAggregateRequest(query).
	//
	//	Load("friends", "AS", "friends").
	//	SortBy("@age", false).
	//	Limit(0, 10).
	//	Filter("@friends>200")
}

// Function to report metrics every 5 seconds
func reportMetrics(interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			snapshot := timer.Snapshot()
			fmt.Printf("[Metrics] Count: %d  Mean: %.2fms  95th: %.2fms  99th: %.2fms  Rate: %.2f/s\n",
				snapshot.Count(),
				float64(snapshot.Mean())/1e6,
				float64(snapshot.Percentile(0.95))/1e6,
				float64(snapshot.Percentile(0.99))/1e6,
				timer.Rate1(),
			)
			timer = metrics.NewTimer()
		}
	}
}

func main() {
	host := flag.String("h", "localhost", "Redis host")
	port := flag.Int("p", 6379, "Redis port")
	password := flag.String("a", "", "Redis password")
	flag.Parse()

	numWorkers := 20
	numQueries := 100000
	countPerThread := numQueries / numWorkers

	// Initialize the index in Redis
	index(*host, *port, *password)

	// Start reporting metrics in a separate goroutine
	done := make(chan struct{})
	go reportMetrics(5*time.Second, done)

	for {
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)

			client := redis.NewClient(&redis.Options{
				Addr:     fmt.Sprintf("%s:%d", *host, *port),
				Password: *password,
				DB:       0,
				Protocol: 2,
			})

			go queryRedis(ctx, client, i, countPerThread, &wg)
		}

		wg.Wait()
	}
}
