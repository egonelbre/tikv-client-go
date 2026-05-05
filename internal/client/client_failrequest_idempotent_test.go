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

package client

import (
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

// recordingExecutor is a minimal async.Executor that records appended tasks
// without running them. It is used to count how many times a Callback was
// scheduled.
type recordingExecutor struct {
	mu    sync.Mutex
	tasks []func()
}

func (e *recordingExecutor) Go(f func())         { e.Append(f) }
func (e *recordingExecutor) Append(fs ...func()) { e.mu.Lock(); e.tasks = append(e.tasks, fs...); e.mu.Unlock() }
func (e *recordingExecutor) count() int          { e.mu.Lock(); defer e.mu.Unlock(); return len(e.tasks) }

func newTestBatchCommandsClient() *batchCommandsClient {
	return &batchCommandsClient{
		target:  "test-target",
		connIdx: "0",
		metrics: initBatchCommandsClientMetrics("test-target", "0"),
	}
}

// TestFailRequestIdempotent_SyncDoubleClose verifies that calling failRequest
// twice on a sync entry is safe. Before the fix the second call panics with
// "close of closed channel" because failRequest unconditionally invokes
// entry.error -> close(b.res).
func TestFailRequestIdempotent_SyncDoubleClose(t *testing.T) {
	c := newTestBatchCommandsClient()

	entry := &batchCommandsEntry{
		res: make(chan *tikvpb.BatchCommandsResponse_Response, 1),
	}
	const requestID uint64 = 42
	c.batched.Store(requestID, entry)
	c.sent.Add(1)

	first := errors.New("first failure")
	c.failRequest(first, requestID, entry)
	require.Equal(t, int64(0), c.sent.Load())

	// Second call must not panic. Without the fix this panics with
	// "close of closed channel" inside entry.error -> close(b.res).
	require.NotPanics(t, func() {
		c.failRequest(errors.New("second failure"), requestID, entry)
	}, "failRequest must be idempotent on sync entries")

	// And it must not double-decrement c.sent on the duplicate path.
	require.Equal(t, int64(0), c.sent.Load(),
		"duplicate failRequest must not decrement c.sent again")

	// The error stored on the entry should reflect the first call.
	require.Equal(t, first, entry.err)
}

// TestFailRequestIdempotent_DoubleErrorOnAsyncEntry verifies that calling
// entry.error twice on an async entry does not schedule the callback twice
// and does not overwrite entry.err.
func TestFailRequestIdempotent_DoubleErrorOnAsyncEntry(t *testing.T) {
	exec := &recordingExecutor{}
	cb := async.NewCallback(exec, func(*tikvrpc.Response, error) {})
	entry := &batchCommandsEntry{cb: cb}

	first := errors.New("first")
	entry.error(first)

	require.NotPanics(t, func() { entry.error(errors.New("second")) },
		"entry.error must be safe to call more than once")
	require.Equal(t, first, entry.err,
		"entry.err must keep the first error")
	require.Equal(t, 1, exec.count(),
		"async callback must be scheduled at most once per entry")
}

// TestFailRequestIdempotent_DoubleErrorOnSyncEntry verifies that calling
// entry.error twice on a sync entry is a no-op on the second call. Before
// the fix the second call panics with "close of closed channel".
func TestFailRequestIdempotent_DoubleErrorOnSyncEntry(t *testing.T) {
	entry := &batchCommandsEntry{
		res: make(chan *tikvpb.BatchCommandsResponse_Response, 1),
	}

	first := errors.New("first")
	entry.error(first)
	require.NotPanics(t, func() { entry.error(errors.New("second")) },
		"sync entry.error must be idempotent (no double close)")
	require.Equal(t, first, entry.err)

	// The channel must be closed exactly once and remain closed.
	_, ok := <-entry.res
	require.False(t, ok, "entry.res must be closed")
}

// TestFailRequestIdempotent_AsyncCancelThenClose verifies that the cancellation
// path used by client_async.go's context.AfterFunc removes the entry from
// c.batched and decrements c.sent, so a subsequent failAsyncRequestsOnClose
// does not double-fire the entry.
//
// Before the fix the cancel hook only calls entry.error and leaves the entry
// live in c.batched; failAsyncRequestsOnClose then re-fires it (overwriting
// entry.err and decrementing c.sent again).
func TestFailRequestIdempotent_AsyncCancelThenClose(t *testing.T) {
	c := newTestBatchCommandsClient()

	exec := &recordingExecutor{}
	cb := async.NewCallback(exec, func(*tikvrpc.Response, error) {})
	entry := &batchCommandsEntry{cb: cb}
	const requestID uint64 = 7
	entry.requestID.Store(requestID)
	c.batched.Store(requestID, entry)
	c.sent.Add(1)

	// Simulate the async cancellation hook in client_async.go: it must
	// publish entry.error AND remove the entry from c.batched / decrement
	// c.sent so that a subsequent failAsyncRequestsOnClose does not see it.
	cancelErr := errors.New("ctx canceled")
	c.cancelAsyncEntry(entry, cancelErr)

	_, stillBatched := c.batched.Load(requestID)
	require.False(t, stillBatched,
		"cancel hook must remove entry from c.batched")
	require.Equal(t, int64(0), c.sent.Load(),
		"cancel hook must decrement c.sent")

	// Now simulate Close() running failAsyncRequestsOnClose. It must not
	// re-process the entry: c.sent stays at 0, the callback is not
	// scheduled a second time, and entry.err keeps the first error.
	c.failAsyncRequestsOnClose()

	require.Equal(t, int64(0), c.sent.Load(),
		"failAsyncRequestsOnClose must not double-decrement c.sent after cancel")
	require.Equal(t, cancelErr, entry.err,
		"failAsyncRequestsOnClose must not overwrite entry.err after cancel")
	require.Equal(t, 1, exec.count(),
		"callback must be scheduled exactly once across cancel and close")
}
