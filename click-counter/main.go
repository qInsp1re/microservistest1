package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ClickEvent struct {
	Timestamp time.Time
	BannerID  int
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = ":3000"
	}
	batchIntervalStr := os.Getenv("BATCH_INTERVAL")
	if batchIntervalStr == "" {
		batchIntervalStr = "1s"
	}
	batchInterval, err := time.ParseDuration(batchIntervalStr)
	if err != nil {
		log.Fatalf("Invalid BATCH_INTERVAL: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Fatalf("Unable to parse DATABASE_URL: %v", err)
	}
	cfg.MaxConns = 100
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	clicksCh := make(chan ClickEvent, 10000)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(batchInterval)
		defer ticker.Stop()

		buffer := make(map[int]map[time.Time]int)
		for {
			select {
			case ev, ok := <-clicksCh:
				if !ok {
					flushBatch(pool, buffer)
					return
				}
				if buffer[ev.BannerID] == nil {
					buffer[ev.BannerID] = make(map[time.Time]int)
				}
				buffer[ev.BannerID][ev.Timestamp]++

			case <-ticker.C:
				flushBatch(pool, buffer)
				buffer = make(map[int]map[time.Time]int)
			}
		}
	}()

	r := chi.NewRouter()
	r.Get("/counter/{id}", func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := strconv.Atoi(idStr)
		if err != nil || id <= 0 {
			http.Error(w, "invalid banner id", http.StatusBadRequest)
			return
		}

		now := time.Now().UTC().Truncate(time.Minute)
		clicksCh <- ClickEvent{Timestamp: now, BannerID: id}
		log.Printf("received click for banner %d at %s", id, now.Format(time.RFC3339))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	r.Post("/stats/{id}", func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := strconv.Atoi(idStr)
		if err != nil || id <= 0 {
			http.Error(w, "invalid banner id", http.StatusBadRequest)
			return
		}

		type reqBody struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		var rb reqBody
		if err := json.NewDecoder(r.Body).Decode(&rb); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		fromTs, err := time.Parse(time.RFC3339, rb.From)
		if err != nil {
			http.Error(w, "invalid from timestamp", http.StatusBadRequest)
			return
		}
		toTs, err := time.Parse(time.RFC3339, rb.To)
		if err != nil {
			http.Error(w, "invalid to timestamp", http.StatusBadRequest)
			return
		}

		rows, err := pool.Query(context.Background(),
			`SELECT ts, count FROM banner_stats WHERE banner_id=$1 AND ts BETWEEN $2 AND $3 ORDER BY ts`,
			id, fromTs.Truncate(time.Minute), toTs.Truncate(time.Minute))
		if err != nil {
			http.Error(w, "DB query error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type stat struct {
			Ts    time.Time `json:"ts"`
			Value int       `json:"v"`
		}
		var stats []stat
		for rows.Next() {
			var ts time.Time
			var cnt int
			rows.Scan(&ts, &cnt)
			stats = append(stats, stat{Ts: ts, Value: cnt})
		}

		json.NewEncoder(w).Encode(map[string]interface{}{"stats": stats})
	})

	log.Printf("listening on %s", port)
	http.ListenAndServe(port, r)

	// Cleanup
	close(clicksCh)
	wg.Wait()
}

func flushBatch(pool *pgxpool.Pool, buffer map[int]map[time.Time]int) {
	var args []interface{}
	var placeholders []string
	argIdx := 1
	for bannerID, tsMap := range buffer {
		for ts, cnt := range tsMap {
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d)", argIdx, argIdx+1, argIdx+2))
			args = append(args, ts, bannerID, cnt)
			argIdx += 3
		}
	}
	if len(placeholders) == 0 {
		return
	}
	query := fmt.Sprintf(
		"INSERT INTO banner_stats(ts,banner_id,count) VALUES %s ON CONFLICT(ts,banner_id) DO UPDATE SET count = banner_stats.count + EXCLUDED.count",
		strings.Join(placeholders, ","),
	)
	if _, err := pool.Exec(context.Background(), query, args...); err != nil {
		log.Printf("batch flush error: %v", err)
		return
	}
	log.Printf("batch flush: %d events into %d rows", len(args)/3, len(placeholders))
}
