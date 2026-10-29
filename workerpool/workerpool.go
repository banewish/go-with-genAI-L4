// Package workerpool реалізує Частину 3 домашньої роботи: пул
// воркерів з обмеженим часом на кожне завдання, що симулює
// конкурентне отримання розміру URL.
package workerpool

import (
	"context"
	"sync"
	"time"
)

// Job — одна одиниця роботи для пулу. Fetch отримує контекст із
// дедлайном і має сам його поважати (кооперативне скасування) —
// саме так уникають витоку горутин при тайм-ауті.
type Job struct {
	ID    string
	Fetch func(ctx context.Context) (int, error)
}

// Result — результат виконання одного Job.
type Result struct {
	JobID string
	Size  int
	Err   error
}

// RunPool запускає рівно numWorkers горутин-воркерів, які беруть
// завдання з jobs і надсилають Result у повернутий канал. Кожному
// Job надається не більше timeout часу — якщо job.Fetch не встигає,
// Result.Err міститиме помилку тайм-ауту (context.DeadlineExceeded).
//
// Реалізація:
//   - Стартує рівно numWorkers горутин, кожна з wg.Add(1)
//   - Кожен воркер у циклі `for job := range jobs` для кожного job:
//     * створює ctx, cancel := context.WithTimeout(context.Background(), timeout)
//     * викликає job.Fetch(ctx)
//     * викликає cancel() (без defer, бо потрібно звільнити таймер відразу)
//     * надсилає Result{JobID: job.ID, Size: size, Err: err} у results
//   - В окремій горутині чекає wg.Wait() і закриває results
//
// Це реалізація select + time.After / context.WithTimeout патерну
// проти витоку горутин. Скасування кооперативне: Fetch сам перевіряє
// ctx.Done() (дивіться приклад slowFetch у тестах).
func RunPool(jobs <-chan Job, numWorkers int, timeout time.Duration) <-chan Result {
	results := make(chan Result)
	
	var wg sync.WaitGroup
	
	// Start exactly numWorkers goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			// Each worker processes jobs from the channel
			for job := range jobs {
				// Create context with timeout for this job
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				
				// Execute the fetch function with timeout
				size, err := job.Fetch(ctx)
				
				// Always cancel to release the timer
				cancel()
				
				// Send result
				results <- Result{
					JobID: job.ID,
					Size:  size,
					Err:   err,
				}
			}
		}()
	}
	
	// Close results channel after all workers finish
	go func() {
		wg.Wait()
		close(results)
	}()
	
	return results
}
