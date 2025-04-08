package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/rcrowley/go-metrics"
	"github.com/redis/go-redis/v9"
)

type Person struct {
	Firstname        string `json:"firstname"`
	Lastname         string `json:"lastname"`
	City             string `json:"city"`
	Country          string `json:"country"`
	Email            string `json:"email"`
	Age              int    `json:"age"`
	Score            int    `json:"score"`
	Friends          int    `json:"friends"`
	RegistrationDate int64  `json:"registrationDate"`
	ConnectionDate   int64  `json:"connectionDate"`
}

var timer = metrics.NewTimer()

func randomTimestamp() int64 {
	start := time.Now().AddDate(-2, 0, 0).Unix()
	end := time.Now().Unix()
	return rand.Int63n(end-start) + start
}

func generatePerson() Person {
	return Person{
		Firstname:        gofakeit.FirstName(),
		Lastname:         gofakeit.LastName(),
		City:             gofakeit.City(),
		Country:          gofakeit.Country(),
		Email:            gofakeit.Email(),
		Age:              rand.Intn(98) + 1,
		Score:            rand.Intn(101),
		Friends:          rand.Intn(501),
		RegistrationDate: randomTimestamp(),
		ConnectionDate:   randomTimestamp(),
	}
}

func insertIntoRedis(ctx context.Context, client *redis.Client, startIndex, count, batchSize int, wg *sync.WaitGroup) {
	defer wg.Done()

	pipe := client.Pipeline()
	for i := 0; i < count; i++ {
		person := generatePerson()
		data, _ := json.Marshal(person)
		key := fmt.Sprintf("person:%d", startIndex+i)
		pipe.Do(ctx, "JSON.SET", key, "$", string(data))

		if (i+1)%batchSize == 0 {
			start := time.Now()
			_, err := pipe.Exec(ctx)
			timer.UpdateSince(start)

			if err != nil {
				log.Println("Pipeline error:", err)
			}
		}
	}
	// Final batch
	if count%batchSize != 0 {
		start := time.Now()
		_, err := pipe.Exec(ctx)
		timer.UpdateSince(start)

		if err != nil {
			log.Println("Final pipeline error:", err)
		}
	}
}

func reportMetrics(interval time.Duration, batchSize int, done <-chan struct{}) {
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
				timer.Rate1()*float64(batchSize),
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

	totalJSON := 2_000_000
	batchSize := 100
	numWorkers := 20
	countPerWorker := totalJSON / numWorkers
	remainder := totalJSON % numWorkers

	//rand.Seed(time.Now().UnixNano())
	gofakeit.Seed(time.Now().UnixNano())

	ctx := context.Background()
	done := make(chan struct{})
	go reportMetrics(5*time.Second, batchSize, done)

	for {
		var wg sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			startIndex := i * countPerWorker
			count := countPerWorker
			if i < remainder {
				count++
			}
			wg.Add(1)

			client := redis.NewClient(&redis.Options{
				Addr:     fmt.Sprintf("%s:%d", *host, *port),
				Password: *password,
				DB:       0,
			})

			go insertIntoRedis(ctx, client, startIndex, count, batchSize, &wg)
		}

		wg.Wait()
	}
}
