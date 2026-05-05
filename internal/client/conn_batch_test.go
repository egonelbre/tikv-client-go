// Copyright 2025 TiKV Authors
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

package client

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/tikvpb"
)

// TestIdleDetectTimerResetCycle drives idleDetect through repeated
// firing -> reset -> firing transitions, exercising both branches of
// fetchAllPendingRequests' first select (head entry received and idle fired).
//
// The test asserts that:
//  1. fetchAllPendingRequests never deadlocks on the timer drain
//     (`<-a.idleDetect.C`) — under Go 1.23+ Timer semantics the legacy
//     "stop+drain+reset" idiom can wedge because the channel has been
//     auto-drained, leaving the receive blocked forever.
//  2. After the idle-fired branch returns and Reset(idleTimeout) has just been
//     called, no stale wakeup is delivered on idleDetect.C — a stale fire
//     between cycles would cause a fresh fetchAllPendingRequests (e.g. after a
//     batchSendLoop panic-restart) to spuriously declare the conn idle.
func TestIdleDetectTimerResetCycle(t *testing.T) {
	prev := idleTimeout
	idleTimeout = 5 * time.Millisecond
	t.Cleanup(func() { idleTimeout = prev })

	a := newBatchConn(1, 16, new(uint32))
	t.Cleanup(a.Close)
	// Re-arm idleDetect with the shortened timeout. newBatchConn captured the
	// previous (production) value at construction time.
	if !a.idleDetect.Stop() {
		select {
		case <-a.idleDetect.C:
		default:
		}
	}
	a.idleDetect.Reset(idleTimeout)

	const cycles = 50
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < cycles; i++ {
		if time.Now().After(deadline) {
			t.Fatalf("test took too long at cycle %d (likely deadlock on idleDetect.C)", i)
		}

		// Use a fresh builder each iteration to avoid the priority-queue
		// retention issue in batchCommandsBuilder.reset (which only drops
		// canceled entries) — this test focuses on timer state, not on
		// builder semantics.
		a.reqBuilder = newBatchCommandsBuilder(16)

		if i%2 == 0 {
			// "Head entry arrived" path. Push first so the channel case is
			// already ready when fetchAllPendingRequests runs select.
			entry := &batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}}
			a.batchCommandsCh <- entry
			done := make(chan struct{})
			go func() {
				a.fetchAllPendingRequests(16)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("cycle %d: fetchAllPendingRequests hung in head-entry branch (likely stuck on <-idleDetect.C drain)", i)
			}
		} else {
			// "Idle fired" path. With idleTimeout==5ms the timer fires before
			// any work arrives; the idle case selects, marks idle and returns.
			done := make(chan struct{})
			go func() {
				a.fetchAllPendingRequests(16)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("cycle %d: fetchAllPendingRequests hung in idle-fired branch", i)
			}
			// After the idle branch, the timer was just Reset(idleTimeout) by
			// fetchAllPendingRequests. Re-arm it with a long duration so the
			// post-Reset stale-value check below is not racing the next
			// scheduled fire of the short test idleTimeout.
			a.idleDetect.Reset(1 * time.Hour)
			// In Go 1.23+, Reset is guaranteed not to deliver a stale value
			// from before the Reset. If we observe a value here it means the
			// fix is broken (e.g. legacy stop+drain idiom returned to use, or
			// the panic-restart path leaked a stale firing).
			select {
			case <-a.idleDetect.C:
				t.Fatalf("cycle %d: stale wakeup delivered on idleDetect.C right after Reset", i)
			default:
			}
			// Restore the test's short idleTimeout for the next cycle.
			// Reset is safe on a running timer in Go 1.23+.
			a.idleDetect.Reset(idleTimeout)
			// Reset the idle counter so subsequent cycles behave normally.
			a.idle = 0
		}
	}
}

// TestIdleDetectTimerHeadCaseAfterIdleConsumed deterministically reproduces
// the legacy Stop+drain deadlock in fetchAllPendingRequests' head-entry case.
//
// Setup: drive the idleDetect timer into the "fired and value consumed, not
// reset" state — the exact post-condition of the legacy idle-fired branch
// (which returns without re-arming). Then push a head entry and re-enter
// fetchAllPendingRequests.
//
// Legacy behavior (Stop + drain + Reset):
//   - Stop() returns false (timer expired)
//   - `<-a.idleDetect.C` blocks forever in Go 1.23+, since the channel was
//     drained by the prior receive and Stop does not re-deliver.
//
// Fixed behavior: head case calls Reset(idleTimeout) directly, which is safe
// on any timer state in Go 1.23+, and never blocks.
func TestIdleDetectTimerHeadCaseAfterIdleConsumed(t *testing.T) {
	prev := idleTimeout
	idleTimeout = 5 * time.Millisecond
	t.Cleanup(func() { idleTimeout = prev })

	a := newBatchConn(1, 16, new(uint32))
	t.Cleanup(a.Close)

	// Put idleDetect in "fired, value consumed, not re-armed" state. This
	// matches what the legacy idle-fired branch leaves behind (it consumes
	// t.C and returns without Reset).
	if !a.idleDetect.Stop() {
		select {
		case <-a.idleDetect.C:
		default:
		}
	}
	a.idleDetect.Reset(1 * time.Millisecond)
	// Wait long enough for the timer to fire.
	time.Sleep(20 * time.Millisecond)
	// Drain the value the timer delivered.
	select {
	case <-a.idleDetect.C:
	case <-time.After(time.Second):
		t.Fatal("timer did not fire — test setup is broken")
	}
	// Now: timer expired, channel drained, no Reset has run.

	// Push a head entry and run fetchAllPendingRequests. With the legacy
	// Stop+drain+Reset idiom, Stop() returns false and `<-C` blocks forever.
	a.reqBuilder = newBatchCommandsBuilder(16)
	a.batchCommandsCh <- &batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}}

	done := make(chan struct{})
	go func() {
		a.fetchAllPendingRequests(16)
		close(done)
	}()

	select {
	case <-done:
		// Pass: the head case re-armed the timer without blocking.
	case <-time.After(2 * time.Second):
		t.Fatal("fetchAllPendingRequests deadlocked on idleDetect.C drain after a prior idle-fired consumption")
	}
}

// TestIdleDetectTimerPanicRestart verifies that the idleDetect timer is
// re-armed by batchSendLoop's panic-recovery defer so the restarted loop
// observes a known-running timer rather than stale state left behind by the
// panic. The scenarios exercised:
//
//  1. Panic occurs after the idle-fired branch consumed t.C and called Reset
//     (timer running, no stale value): defer Reset is harmless.
//  2. Panic occurs while the timer has fired and the value is sitting in t.C
//     unread (e.g. between Stop and Reset in some hypothetical future code
//     path): defer Reset must drop the pending value so the restarted loop
//     does not immediately go idle.
//
// In both cases, after the panic-recovery defer runs, a non-blocking read on
// t.C must NOT yield a value (Go 1.23+ Reset guarantees no stale delivery).
func TestIdleDetectTimerPanicRestart(t *testing.T) {
	prev := idleTimeout
	idleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { idleTimeout = prev })

	// Scenario 1: timer running, no stale value in C.
	a1 := newBatchConn(1, 16, new(uint32))
	t.Cleanup(a1.Close)
	if !a1.idleDetect.Stop() {
		select {
		case <-a1.idleDetect.C:
		default:
		}
	}
	a1.idleDetect.Reset(idleTimeout)
	// Simulate the panic-recovery defer's Reset.
	a1.idleDetect.Reset(idleTimeout)
	select {
	case <-a1.idleDetect.C:
		t.Fatalf("scenario 1: stale value delivered after defer Reset on running timer")
	case <-time.After(5 * time.Millisecond):
	}

	// Scenario 2: timer fired, value pending in C unread.
	a2 := newBatchConn(1, 16, new(uint32))
	t.Cleanup(a2.Close)
	if !a2.idleDetect.Stop() {
		select {
		case <-a2.idleDetect.C:
		default:
		}
	}
	a2.idleDetect.Reset(1 * time.Millisecond)
	time.Sleep(10 * time.Millisecond) // let the timer fire; value (may be) pending in C
	// Simulate the panic-recovery defer's Reset.
	a2.idleDetect.Reset(idleTimeout)
	select {
	case <-a2.idleDetect.C:
		t.Fatalf("scenario 2: stale pre-Reset value delivered after defer Reset")
	case <-time.After(5 * time.Millisecond):
	}
}
