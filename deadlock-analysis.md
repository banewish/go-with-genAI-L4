# Аналіз дедлоку (Частина 2)

## Код, що аналізується

```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	ch := make(chan int) // небуферизований канал

	wg.Add(1)
	go func() {
		// симуляція важкої роботи
		ch <- 42
		wg.Done()
	}()

	wg.Wait()
	fmt.Println(<-ch)
}
```

## Мій промпт до ШІ

Вставте нижче точний текст промпту, який ви розробили. Промпт має
явно вимагати від ШІ:

- **не просто переписати код**, а пояснити точну послідовність
  переплетення (interleaving) горутин, що веде до зависання;
- проаналізувати, чому виклик `wg.Wait()` перед читанням з
  небуферизованого каналу зупиняє виконання.

### Мій промпт

Have a look at this code and identify the exact deadlock issue. Please provide explanation of the goroutine scheduling (interleaving) that causes the hang and explain specifically why `wg.Wait()` is called before anything reads from the unbuffered channel and how this creates the circular wait condition
Also provide a fixed version that eliminates the deadlock and explain why your solution works

## Пояснення від ШІ

Вставте нижче відповідь ШІ-асистента на ваш промпт — покрокове
пояснення переплетення горутин.

### Відповідь ШІ

**The Deadlock Sequence:**

1. **Main goroutine (G_main)**: Calls `wg.Wait()` and blocks immediately, waiting for the WaitGroup counter to reach zero.

2. **Worker goroutine (G_worker)**: Starts and attempts to execute `ch <- 42` (send 42 on the unbuffered channel).

3. **The Circular Wait (Deadlock)**: 
   - G_worker tries to send on `ch`, but nobody is receiving from it yet
   - Unbuffered channels require a sender and receiver to be ready **simultaneously**
   - G_worker blocks waiting for a receiver
   - G_main is blocked on `wg.Wait()`, waiting for `wg.Done()` 
   - `wg.Done()` can never be called because G_worker is blocked on `ch <- 42`
   - Main never reaches `fmt.Println(<-ch)`, so nobody is reading from the channel
   - **Result**: Both goroutines wait for each other infinitely

**Why wg.Wait() Before Reading Creates the Problem:**

The critical issue is the ordering: `wg.Wait()` is called before `<-ch`. This means:
- Main thread blocks on `wg.Wait()` before it can execute `<-ch`
- The worker thread needs `wg.Done()` to be called before it can proceed
- But the worker blocks on `ch <- 42` and can't call `wg.Done()` until something reads from `ch`
- But nothing can read from `ch` because Main is stuck in `wg.Wait()`

**The Fix:**

Simply remove the WaitGroup entirely (it's unnecessary for a single goroutine and one value):

```go
func main() {
	ch := make(chan int)
	go func() {
		ch <- 42
	}()
	fmt.Println(<-ch)  // Read BEFORE waiting
}
```

Or if keeping WaitGroup, read from the channel **before** calling `wg.Wait()`:

```go
func main() {
	var wg sync.WaitGroup
	ch := make(chan int)
	wg.Add(1)
	go func() {
		ch <- 42
		wg.Done()
	}()
	value := <-ch      // Read FIRST - unblocks the worker
	wg.Wait()          // Wait AFTER - worker has already called Done()
	fmt.Println(value)
}
```

The solution works because:
- Reading from the channel **before** `wg.Wait()` removes the circular dependency
- When main executes `<-ch`, it waits for a sender
- Worker sends on `ch <- 42` successfully
- Worker then calls `wg.Done()`
- Main receives the value and proceeds
- No circular wait condition exists

## Мінімальне виправлення

Робочу, тестовану версію виправлення реалізовано в
[`deadlock/fixed.go`](./deadlock/fixed.go) (функція `Run`), а
автоматична перевірка — у `deadlock/fixed_test.go`. Коротко опишіть
своїми словами, яку зміну порядку операцій (чи структури каналу) ви
застосували і чому саме вона усуває циклічне очікування.

### Мінімальне виправлення - пояснення

The fix removes the WaitGroup and rewrite the code to read from the channel immediately after starting the goroutine. Because its eliminating `wg.Wait()` before `<-ch` the main goroutine can now block on the channel read, which allows it to successfully send the value and complete. This approach is simpler and eliminates the deadlock by ensuring the receiver (main) is always waiting before the sender (worker) attempts to send.
