/*
	Basic Script that generates random data to help create lots of files for testing.
*/

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/0xRadioAc7iv/go-bitcask/bitcask"
)

const (
	concurrency = 6

	// Fixed universe
	totalKeys   = 100
	totalValues = 100

	// Per-cycle behavior
	keysPerCycleWrite  = 20
	keysPerCycleDelete = 10
	cyclesPerWorker    = 5000

	sleepBetweenCycles = 10 * time.Millisecond

	progressEvery = 500
)

func main() {
	start := time.Now()
	fmt.Println("Starting Bitcask churn-heavy load generator")

	keys := makeKeys(totalKeys)
	values := makeValues(totalValues)

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorker(id, keys, values)
		}(i)
	}

	wg.Wait()
	fmt.Printf("Load finished in %v\n", time.Since(start))
}

func runWorker(id int, keys []string, values []string) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	client, err := bitcask.Connect()
	if err != nil {
		fmt.Printf("[worker %d] connect error: %v\n", id, err)
		return
	}
	defer client.Close()

	for cycle := 1; cycle <= cyclesPerWorker; cycle++ {

		// ---- WRITE / OVERWRITE PHASE ----
		for i := 0; i < keysPerCycleWrite; i++ {
			key := keys[rng.Intn(len(keys))]
			val := values[rng.Intn(len(values))]

			if _, err := client.SET(key, val); err != nil {
				fmt.Printf("[worker %d] SET error: %v\n", id, err)
				return
			}
		}

		// ---- DELETE PHASE ----
		for i := 0; i < keysPerCycleDelete; i++ {
			key := keys[rng.Intn(len(keys))]

			if _, err := client.DELETE(key); err != nil {
				fmt.Printf("[worker %d] DELETE error: %v\n", id, err)
				return
			}
		}

		// ---- REWRITE PHASE (forces overwrite garbage) ----
		for i := 0; i < keysPerCycleWrite/2; i++ {
			key := keys[rng.Intn(len(keys))]
			val := values[rng.Intn(len(values))]

			if _, err := client.SET(key, val); err != nil {
				fmt.Printf("[worker %d] REWRITE error: %v\n", id, err)
				return
			}
		}

		if cycle%progressEvery == 0 {
			fmt.Printf("[worker %d] completed %d cycles\n", id, cycle)
		}

		if sleepBetweenCycles > 0 {
			time.Sleep(sleepBetweenCycles)
		}
	}
}

func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%03d", i)
	}
	return keys
}

func makeValues(n int) []string {
	values := make([]string, n)
	for i := 0; i < n; i++ {
		values[i] = fmt.Sprintf("value-%03d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)
	}
	return values
}
