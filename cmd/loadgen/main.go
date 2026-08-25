// Command loadgen measures the cluster.
//
//	loadgen --mode=write      --addr=http://localhost:8001 --workers=16
//	loadgen --mode=read       --addr=http://localhost:8002 --consistency=eventual
//	loadgen --mode=staleness  --addr=http://localhost:8001 --follower=http://localhost:8002
//	loadgen --mode=availability --addr=http://localhost:8001 --duration=10s
package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	addr        = flag.String("addr", "http://localhost:8001", "node to hit")
	follower    = flag.String("follower", "http://localhost:8002", "follower, for staleness mode")
	workers     = flag.Int("workers", 16, "concurrent clients")
	duration    = flag.Duration("duration", 5*time.Second, "how long to run")
	mode        = flag.String("mode", "write", "write, read, staleness or availability")
	consistency = flag.String("consistency", "strong", "strong or eventual (read mode)")
	samples     = flag.Int("samples", 200, "number of samples (staleness mode)")
	label       = flag.String("label", "", "printed with the results")
	jitter      = flag.Duration("jitter", 60*time.Millisecond, "random delay between staleness samples")
)

func main() {
	flag.Parse()

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *workers + 4},
	}

	switch *mode {
	case "write", "read":
		throughput(client)
	case "staleness":
		staleness(client)
	case "availability":
		availability(client)
	default:
		fmt.Println("unknown mode:", *mode)
	}
}

// ---------------------------------------------------------------- throughput

// throughput runs `workers` clients flat out and reports ops/sec plus the
// latency distribution. Percentiles matter more than the mean: p99 is what
// your users actually complain about.
func throughput(client *http.Client) {
	if *mode == "read" {
		put(client, *addr, "bench", "seed-value") // make sure the key exists
	}

	var (
		mu      sync.Mutex
		latency []time.Duration
		errs    int
	)

	deadline := time.Now().Add(*duration)
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var local []time.Duration
			localErrs := 0

			for time.Now().Before(deadline) {
				t0 := time.Now()
				err := oneRequest(client, id)
				took := time.Since(t0)

				if err != nil {
					localErrs++
					continue
				}
				local = append(local, took)
			}

			mu.Lock()
			latency = append(latency, local...)
			errs += localErrs
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	report(time.Since(start), latency, errs)
}

func oneRequest(client *http.Client, worker int) error {
	if *mode == "read" {
		url := fmt.Sprintf("%s/kv/bench?consistency=%s", *addr, *consistency)
		return do(client, "GET", url, "")
	}
	url := fmt.Sprintf("%s/kv/worker-%d", *addr, worker)
	return do(client, "PUT", url, "some-value")
}

func report(elapsed time.Duration, lat []time.Duration, errs int) {
	name := *label
	if name == "" {
		name = *mode
	}

	if len(lat) == 0 {
		fmt.Printf("%-28s NO SUCCESSFUL REQUESTS (%d errors)\n", name, errs)
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	pct := func(p float64) time.Duration {
		i := int(float64(len(lat))*p) - 1
		if i < 0 {
			i = 0
		}
		return lat[i]
	}

	fmt.Printf("%-28s %8.0f ops/s   p50 %-9v p99 %-9v max %-9v  (n=%d, errors=%d)\n",
		name,
		float64(len(lat))/elapsed.Seconds(),
		pct(0.50).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond),
		lat[len(lat)-1].Round(time.Microsecond),
		len(lat), errs)
}

// ----------------------------------------------------------------- staleness

// staleness measures the gap between a write being acknowledged by the leader
// and that write becoming visible to an eventual read on a follower.
//
// This is the cost of choosing ?consistency=eventual, in milliseconds.
func staleness(client *http.Client) {
	var windows []time.Duration
	misses := 0

	for i := 0; i < *samples; i++ {
		// Sleep a random slice of a heartbeat before each write.
		//
		// Without this the loop phase-locks to the leader's heartbeat
		// ticker: every write lands at the same point in the cycle, so we
		// only ever sample one part of the distribution. Jitter spreads the
		// writes across the whole interval, which is what a real workload
		// looks like.
		time.Sleep(time.Duration(rand.Int63n(int64(*jitter))))

		value := fmt.Sprintf("v%d-%d", i, time.Now().UnixNano())

		if err := put(client, *addr, "staleness", value); err != nil {
			misses++
			continue
		}
		acked := time.Now()

		// Poll the follower with an EVENTUAL read until it catches up.
		deadline := acked.Add(2 * time.Second)
		for time.Now().Before(deadline) {
			got, err := get(client, *follower, "staleness", "eventual")
			if err == nil && got == value {
				windows = append(windows, time.Since(acked))
				break
			}
		}
	}

	if len(windows) == 0 {
		fmt.Println("staleness: no samples completed")
		return
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })

	pct := func(p float64) time.Duration {
		i := int(float64(len(windows))*p) - 1
		if i < 0 {
			i = 0
		}
		return windows[i]
	}

	fmt.Printf("%-28s p50 %-9v p99 %-9v max %-9v  (n=%d)\n",
		"staleness window",
		pct(0.50).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond),
		windows[len(windows)-1].Round(time.Microsecond),
		len(windows))
}

// sample is one write attempt and whether it worked.
type sample struct {
	at time.Time
	ok bool
}

// -------------------------------------------------------------- availability

// availability writes continuously and records exactly when writes stopped
// working and when they resumed. Run it while killing the leader and the
// longest gap IS your failover time, measured from the client's point of
// view -- which is the only point of view that matters.
func availability(client *http.Client) {
	var (
		mu      sync.Mutex
		history []sample
	)

	deadline := time.Now().Add(*duration)
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var local []sample

			for time.Now().Before(deadline) {
				err := do(client, "PUT",
					fmt.Sprintf("%s/kv/avail-%d", *addr, id), "v")
				local = append(local, sample{at: time.Now(), ok: err == nil})
			}

			mu.Lock()
			history = append(history, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	sort.Slice(history, func(i, j int) bool { return history[i].at.Before(history[j].at) })

	var (
		ok, failed int
		lastOK     = start
		maxGap     time.Duration
		gapStart   time.Time
	)
	for _, s := range history {
		if s.ok {
			if gap := s.at.Sub(lastOK); gap > maxGap {
				maxGap = gap
				gapStart = lastOK
			}
			lastOK = s.at
			ok++
		} else {
			failed++
		}
	}

	fmt.Printf("%-28s %d ok, %d failed over %v\n", "availability",
		ok, failed, time.Since(start).Round(time.Millisecond))
	fmt.Printf("%-28s %v (starting %v into the run)\n", "  longest write outage",
		maxGap.Round(time.Millisecond), gapStart.Sub(start).Round(time.Millisecond))
	fmt.Printf("%-28s %s\n", "  timeline", timeline(history, start, time.Since(start)))
}

// timeline draws one character per slice of the run: # = writes succeeding,
// _ = writes failing. It makes the outage visible at a glance.
func timeline(history []sample, start time.Time, total time.Duration) string {
	const width = 50
	buckets := make([]int, width) // 0 unknown, 1 all ok, -1 any failure

	for _, s := range history {
		i := int(float64(s.at.Sub(start)) / float64(total) * width)
		if i >= width {
			i = width - 1
		}
		if !s.ok {
			buckets[i] = -1
		} else if buckets[i] == 0 {
			buckets[i] = 1
		}
	}

	var b strings.Builder
	for _, v := range buckets {
		switch v {
		case -1:
			b.WriteByte('_')
		case 1:
			b.WriteByte('#')
		default:
			b.WriteByte('.')
		}
	}
	return b.String()
}

// ------------------------------------------------------------------ plumbing

func do(client *http.Client, method, url, body string) error {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain so the connection can be reused

	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func put(client *http.Client, base, key, value string) error {
	return do(client, "PUT", base+"/kv/"+key, value)
}

func get(client *http.Client, base, key, mode string) (string, error) {
	resp, err := client.Get(fmt.Sprintf("%s/kv/%s?consistency=%s", base, key, mode))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return string(body), nil
}
