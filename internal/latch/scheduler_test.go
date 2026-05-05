// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/latch/scheduler_test.go
//

// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package latch

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCloseUnblocksPendingLockWaiters(t *testing.T) {
	sched := NewScheduler(7)

	keys := [][]byte{[]byte("k1")}

	// First Lock acquires the latch successfully.
	first := sched.Lock(getTso(), keys)
	if first.IsStale() {
		t.Fatalf("unexpected stale on first lock")
	}

	// Second Lock on the same key blocks in wg.Wait().
	done := make(chan struct{})
	go func() {
		second := sched.Lock(getTso(), keys)
		_ = second
		close(done)
	}()

	// Give the second goroutine a moment to actually park on wg.Wait().
	time.Sleep(50 * time.Millisecond)

	// Close the scheduler. This must unblock the parked second Lock.
	sched.Close()

	select {
	case <-done:
		// pass
	case <-time.After(5 * time.Second):
		t.Fatalf("Lock() did not return within 5s after Close(); waiter leaked")
	}
}

func TestWithConcurrency(t *testing.T) {
	sched := NewScheduler(7)
	defer sched.Close()

	ch := make(chan [][]byte, 100)
	const workerCount = 10
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(ch <-chan [][]byte, wg *sync.WaitGroup) {
			for txn := range ch {
				lock := sched.Lock(getTso(), txn)
				if lock.IsStale() {
					// Should restart the transaction or return error
				} else {
					lock.SetCommitTS(getTso())
					// Do 2pc
				}
				sched.UnLock(lock)
			}
			wg.Done()
		}(ch, &wg)
	}

	for i := 0; i < 999; i++ {
		ch <- generate()
	}
	close(ch)

	wg.Wait()
}

// generate generates something like:
// {[]byte("a"), []byte("b"), []byte("c")}
// {[]byte("a"), []byte("d"), []byte("e"), []byte("f")}
// {[]byte("e"), []byte("f"), []byte("g"), []byte("h")}
// The data should not repeat in the sequence.
func generate() [][]byte {
	table := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	ret := make([][]byte, 0, 5)
	chance := []int{100, 60, 40, 20}
	for i := 0; i < len(chance); i++ {
		needMore := rand.Intn(100) < chance[i]
		if needMore {
			randBytes := []byte{table[rand.Intn(len(table))]}
			if !contains(randBytes, ret) {
				ret = append(ret, randBytes)
			}
		}
	}
	return ret
}

func contains(x []byte, set [][]byte) bool {
	for _, y := range set {
		if bytes.Equal(x, y) {
			return true
		}
	}
	return false
}
