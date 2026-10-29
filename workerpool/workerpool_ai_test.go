package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunPool_ErrorPropagation перевіряє, що помилки від job.Fetch
// коректно поширюються у Result.Err, включаючи помилки контексту.
func TestRunPool_ErrorPropagation(t *testing.T) {
	jobs := make(chan Job, 3)

	// Job 1: успішна виконання
	jobs <- Job{
		ID: "success",
		Fetch: func(ctx context.Context) (int, error) {
			return 100, nil
		},
	}

	// Job 2: користувацька помилка
	customErr := errors.New("custom fetch error")
	jobs <- Job{
		ID: "custom-error",
		Fetch: func(ctx context.Context) (int, error) {
			return 0, customErr
		},
	}

	// Job 3: timeout
	jobs <- Job{
		ID: "timeout",
		Fetch: func(ctx context.Context) (int, error) {
			select {
			case <-time.After(5 * time.Second):
				return 999, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		},
	}
	close(jobs)

	results := RunPool(jobs, 2, 100*time.Millisecond)

	resultMap := make(map[string]Result)
	for r := range results {
		resultMap[r.JobID] = r
	}

	// Check success result
	if r, ok := resultMap["success"]; !ok {
		t.Fatal("success job not found in results")
	} else if r.Err != nil {
		t.Errorf("success: unexpected error: %v", r.Err)
	} else if r.Size != 100 {
		t.Errorf("success: Size = %d, want 100", r.Size)
	}

	// Check custom error
	if r, ok := resultMap["custom-error"]; !ok {
		t.Fatal("custom-error job not found in results")
	} else if r.Err != customErr {
		t.Errorf("custom-error: Err = %v, want %v", r.Err, customErr)
	}

	// Check timeout error
	if r, ok := resultMap["timeout"]; !ok {
		t.Fatal("timeout job not found in results")
	} else if r.Err == nil {
		t.Error("timeout: expected context.DeadlineExceeded, got nil")
	} else if !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Errorf("timeout: Err = %v, want context.DeadlineExceeded", r.Err)
	}
}

// TestRunPool_WorkerLoadBalancing перевіряє, що завдання розподіляються
// між воркерами більш-менш рівномірно, а не всі йдуть до одного воркера.
func TestRunPool_WorkerLoadBalancing(t *testing.T) {
	const numWorkers = 4
	const numJobs = 12

	var completedCount int64
	jobDone := func(ctx context.Context) (int, error) {
		atomic.AddInt64(&completedCount, 1)
		select {
		case <-time.After(50 * time.Millisecond):
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	jobs := make(chan Job, numJobs)
	for i := 0; i < numJobs; i++ {
		jobs <- Job{
			ID:    fmt.Sprintf("job-%d", i),
			Fetch: jobDone,
		}
	}
	close(jobs)

	start := time.Now()
	results := RunPool(jobs, numWorkers, time.Second)

	// Consume all results
	count := 0
	for range results {
		count++
	}

	elapsed := time.Since(start)

	if count != numJobs {
		t.Errorf("got %d results, want %d", count, numJobs)
	}

	// With 4 workers processing 12 jobs of 50ms each concurrently:
	// Expected time ≈ 50ms (first batch) + 50ms (second) + 50ms (third) = 150ms
	// Should NOT be 600ms (sequential)
	if elapsed > 400*time.Millisecond {
		t.Errorf("elapsed time %v suggests sequential execution, should be ~150-250ms", elapsed)
	}

	if atomic.LoadInt64(&completedCount) != int64(numJobs) {
		t.Errorf("completed jobs = %d, want %d", atomic.LoadInt64(&completedCount), numJobs)
	}
}

// TestRunPool_EmptyJobsChannel перевіряє, що пул коректно завершується,
// коли канал з завданнями порожній чи закритий.
func TestRunPool_EmptyJobsChannel(t *testing.T) {
	jobs := make(chan Job)
	close(jobs)

	results := RunPool(jobs, 3, time.Second)

	// Should receive no results and channel should close immediately
	count := 0
	for range results {
		count++
	}

	if count != 0 {
		t.Errorf("got %d results, want 0 for empty jobs channel", count)
	}
}

// TestRunPool_SingleWorkerBehavior перевіряє, що пул з одним воркером
// все ще обробляє всі завдання послідовно.
func TestRunPool_SingleWorkerBehavior(t *testing.T) {
	const numJobs = 5

	jobs := make(chan Job, numJobs)
	for i := 0; i < numJobs; i++ {
		jobID := i
		jobs <- Job{
			ID: fmt.Sprintf("job-%d", jobID),
			Fetch: func(ctx context.Context) (int, error) {
				select {
				case <-time.After(10 * time.Millisecond):
					return jobID, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			},
		}
	}
	close(jobs)

	results := RunPool(jobs, 1, time.Second)

	count := 0
	for r := range results {
		if r.Err != nil {
			t.Errorf("job %s: unexpected error: %v", r.JobID, r.Err)
		}
		count++
	}

	if count != numJobs {
		t.Errorf("got %d results, want %d", count, numJobs)
	}
}

// TestRunPool_ContextCancellationCooperative перевіряє, що Fetch функції,
// які кооперативно поважають ctx.Done(), дійсно реагують на скасування.
func TestRunPool_ContextCancellationCooperative(t *testing.T) {
	var cancelledCount int32

	slowButResponsive := func(ctx context.Context) (int, error) {
		// Simulates a job that would take 5 seconds, but can be interrupted
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Check context periodically (good practice)
				continue
			case <-ctx.Done():
				atomic.AddInt32(&cancelledCount, 1)
				return 0, ctx.Err()
			}
		}
	}

	jobs := make(chan Job, 3)
	for i := 0; i < 3; i++ {
		jobs <- Job{
			ID:    fmt.Sprintf("job-%d", i),
			Fetch: slowButResponsive,
		}
	}
	close(jobs)

	// Very short timeout - should interrupt all jobs
	results := RunPool(jobs, 1, 20*time.Millisecond)

	cancelledErrors := 0
	for r := range results {
		if errors.Is(r.Err, context.DeadlineExceeded) {
			cancelledErrors++
		}
	}

	if cancelledErrors != 3 {
		t.Errorf("cancelled jobs = %d, want 3", cancelledErrors)
	}

	if c := atomic.LoadInt32(&cancelledCount); c != 3 {
		t.Errorf("jobs that detected cancellation = %d, want 3", c)
	}
}

// TestRunPool_VariousTimeout перевіряє, що різні таймаути для різних
// пулів працюють коректно.
func TestRunPool_VariousTimeout(t *testing.T) {
	tests := []struct {
		name          string
		timeout       time.Duration
		jobDuration   time.Duration
		shouldTimeout bool
	}{
		{"generous timeout", 500 * time.Millisecond, 50 * time.Millisecond, false},
		{"tight timeout", 10 * time.Millisecond, 100 * time.Millisecond, true},
		{"zero timeout", 0, 1 * time.Millisecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs := make(chan Job, 1)
			jobs <- Job{
				ID: "test-job",
				Fetch: func(ctx context.Context) (int, error) {
					select {
					case <-time.After(tt.jobDuration):
						return 42, nil
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				},
			}
			close(jobs)

			results := RunPool(jobs, 1, tt.timeout)

			var result Result
			for r := range results {
				result = r
			}

			hasTimeout := errors.Is(result.Err, context.DeadlineExceeded)
			if hasTimeout != tt.shouldTimeout {
				t.Errorf("shouldTimeout = %v, got %v (Err = %v)", tt.shouldTimeout, hasTimeout, result.Err)
			}
		})
	}
}
