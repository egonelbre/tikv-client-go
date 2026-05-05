// Copyright 2026 TiKV Authors
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

package transaction

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTTLManagerCloseDoesNotPanicConcurrentlyWithRun exercises the
// run/close race that, without proper synchronization between the
// state transition and the channel allocation, lets close() observe
// state==Running while tm.ch is still nil and panic with
// "close of nil channel".
//
// The test launches many goroutines that race tm.tryStart() (the
// state+channel transition extracted from run()) against tm.close()
// and tm.reset() and asserts no goroutine ever panics on close-of-nil.
func TestTTLManagerCloseDoesNotPanicConcurrentlyWithRun(t *testing.T) {
	const iterations = 2000
	for i := 0; i < iterations; i++ {
		var tm ttlManager
		var wg sync.WaitGroup
		var panicked atomic.Bool

		safeCall := func(f func()) {
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
					t.Errorf("panic: %v", r)
				}
			}()
			f()
		}

		wg.Add(3)
		go func() {
			defer wg.Done()
			safeCall(func() {
				_, _, _ = tm.tryStart(nil)
			})
		}()
		go func() {
			defer wg.Done()
			safeCall(func() { tm.close() })
		}()
		go func() {
			defer wg.Done()
			safeCall(func() { tm.reset() })
		}()
		wg.Wait()
		if panicked.Load() {
			t.FailNow()
		}
	}
}

// TestTTLManagerStaleKeepAliveCannotCloseNewIncarnation simulates a
// keepAlive goroutine that survived a reset() and a subsequent run()
// of the same ttlManager. The stale keepAlive's call to close must
// not shut down the freshly-started ttlManager.
//
// The expected ordering is:
//  1. run #1 starts the ttlManager (gen=1).
//  2. reset stops it; the gen-1 keepAlive is still alive but slow.
//  3. run #2 starts a new incarnation (gen=2).
//  4. The stale gen-1 keepAlive eventually calls
//     closeFromKeepAlive(1).
//  5. The new (gen=2) channel must remain open; tm.state must remain
//     Running.
func TestTTLManagerStaleKeepAliveCannotCloseNewIncarnation(t *testing.T) {
	var tm ttlManager

	// Start gen 1.
	ch1, gen1, started := tm.tryStart(nil)
	require.True(t, started, "first tryStart must start")
	require.NotNil(t, ch1)
	require.Equal(t, uint64(1), gen1)

	// Reset (simulating the old keepAlive being told to stop).
	tm.reset()
	// The gen-1 channel is now closed.
	select {
	case <-ch1:
	default:
		t.Fatal("reset did not close the gen-1 channel")
	}

	// Start gen 2 — a fresh incarnation. The old keepAlive goroutine
	// is still alive in this thought experiment.
	ch2, gen2, started := tm.tryStart(nil)
	require.True(t, started, "second tryStart must start")
	require.NotNil(t, ch2)
	require.NotEqual(t, gen1, gen2)
	require.NotEqual(t, ch1, ch2)

	// The stale keepAlive (gen-1) now triggers a close on tm. With
	// the generation-aware fix this must be a no-op.
	tm.closeFromKeepAlive(gen1)

	// The new (gen-2) channel must still be open.
	select {
	case <-ch2:
		t.Fatal("stale keepAlive closed the new ttlManager's channel")
	default:
	}

	// State must still be Running for gen-2.
	tm.mu.Lock()
	state := tm.state
	curGen := tm.gen
	tm.mu.Unlock()
	assert.Equal(t, stateRunning, state, "new incarnation must remain Running")
	assert.Equal(t, gen2, curGen)

	// Sanity: the live keepAlive (gen-2) can close it.
	tm.closeFromKeepAlive(gen2)
	select {
	case <-ch2:
	default:
		t.Fatal("live keepAlive failed to close its own channel")
	}
}

// TestTTLManagerRunCloseResetCycle stresses the full
// run/close/reset/run cycle to ensure interleavings between a stale
// keepAlive close and a fresh run never close the new channel and
// never panic. The stale-keepAlive close races against a fresh
// incarnation that has already been started; the fix must ensure the
// stale close cannot affect ch2.
func TestTTLManagerRunCloseResetCycle(t *testing.T) {
	const cycles = 500
	for i := 0; i < cycles; i++ {
		var tm ttlManager

		ch1, gen1, ok := tm.tryStart(nil)
		require.True(t, ok)

		// Reset (this is what would prompt the stale keepAlive to
		// notice and stop, but in our scenario the stale goroutine
		// is slow and hasn't yet seen the channel close).
		tm.reset()

		// Start a fresh incarnation BEFORE the stale close racer.
		ch2, gen2, ok := tm.tryStart(nil)
		require.True(t, ok)

		var wg sync.WaitGroup
		wg.Add(2)

		// The stale keepAlive races to close using its old
		// generation. With the fix it must never close ch2.
		go func() {
			defer wg.Done()
			tm.closeFromKeepAlive(gen1)
		}()

		// Concurrently, the live (gen-2) keepAlive may also be
		// observing things; it should be a no-op as long as state
		// is Running. Add another reader so the race detector
		// exercises the field accesses.
		go func() {
			defer wg.Done()
			tm.mu.Lock()
			_ = tm.state
			_ = tm.gen
			tm.mu.Unlock()
		}()

		wg.Wait()

		// ch1 must be closed (by reset).
		select {
		case <-ch1:
		default:
			t.Fatalf("iter %d: ch1 not closed", i)
		}
		// ch2 must be open — stale keepAlive must not have closed
		// the new incarnation.
		select {
		case <-ch2:
			t.Fatalf("iter %d: stale keepAlive closed new incarnation", i)
		default:
		}

		// Cleanup so we don't leak.
		tm.closeFromKeepAlive(gen2)
	}
}
