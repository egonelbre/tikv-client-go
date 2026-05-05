package txnlock

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/util"
)

// TestLockResolverCache is used to cover the issue https://github.com/pingcap/tidb/issues/59494.
func TestLockResolverCache(t *testing.T) {
	util.EnableFailpoints()
	lockResolver := NewLockResolver(nil)
	lock := func(key, primary string, startTS uint64, useAsyncCommit bool, secondaries [][]byte) *kvrpcpb.LockInfo {
		return &kvrpcpb.LockInfo{
			Key:            []byte(key),
			PrimaryLock:    []byte(primary),
			LockVersion:    startTS,
			UseAsyncCommit: useAsyncCommit,
			MinCommitTs:    startTS + 1,
			Secondaries:    secondaries,
		}
	}

	resolvedTxnTS := uint64(1)
	k1 := "k1"
	k2 := "k2"
	resolvedTxnStatus := TxnStatus{
		ttl:         0,
		commitTS:    10,
		primaryLock: lock(k1, k1, resolvedTxnTS, true, [][]byte{[]byte(k2)}),
	}
	lockResolver.mu.resolved[resolvedTxnTS] = resolvedTxnStatus
	toResolveLock := lock(k2, k1, resolvedTxnTS, true, [][]byte{})
	backOff := retry.NewBackoffer(context.Background(), asyncResolveLockMaxBackoff)

	// Save the async commit transaction resolved result to the resolver cache.
	lockResolver.saveResolved(resolvedTxnTS, resolvedTxnStatus)

	// With failpoint, the async commit transaction will be resolved and `CheckSecondaries` would not be called.
	// Otherwise, the test would panic as the storage is nil.
	require.Nil(t, failpoint.Enable("tikvclient/resolveAsyncCommitLockReturn", "return"))
	_, err := lockResolver.ResolveLocks(backOff, 5, []*Lock{NewLock(toResolveLock)})
	require.NoError(t, err)
	require.Nil(t, failpoint.Disable("tikvclient/resolveAsyncCommitLockReturn"))
}

func TestTryAsyncResolve(t *testing.T) {
	require.Equal(t, AsyncResolveLockSemaphoreLimit, cap(globalAsyncResolveLockSemaphore))
	mockMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_try_async_resolve_running_tasks",
		Help: "Test gauge for TestTryAsyncResolve",
	})
	checkMetricVal := func(v float64) {
		m := &dto.Metric{}
		require.NoError(t, mockMetric.Write(m))
		require.Equal(t, v, m.GetGauge().GetValue())
	}

	// default lock resolver should use the global async resolve lock semaphore
	lockResolver := NewLockResolver(nil)
	assert.Equal(t, cap(lockResolver.asyncResolvePool.semaphore), cap(globalAsyncResolveLockSemaphore))
	assert.Equal(t, 0, len(globalAsyncResolveLockSemaphore))

	exitLatches := make([]chan struct{}, 0, 16)
	tryAsync := func() (isAsync bool) {
		enterLatch := make(chan struct{})
		exitLatch := make(chan struct{})

		isAsync = lockResolver.asyncResolvePool.tryAsyncResolve(func() {
			close(enterLatch)
			<-exitLatch
		}, mockMetric)

		if isAsync {
			exitLatches = append(exitLatches, exitLatch)
		}

		return isAsync
	}

	exitTask := func(idx int) {
		close(exitLatches[idx])
		exitLatches[idx] = nil
	}

	defer func() {
		// clean up
		for _, l := range exitLatches {
			if l != nil {
				close(l)
			}
		}
		lockResolver.Close()
	}()

	// close old pool and mock a custom asyncResolveLockSemaphore with limit 5
	lockResolver.asyncResolvePool.Close()
	lockResolver.asyncResolvePool = newAsyncResolveTaskPool(make(chan struct{}, 5))
	waitSemaphoreSizeWithCheck := func(cnt int) {
		assert.Eventually(t, func() bool {
			return len(lockResolver.asyncResolvePool.semaphore) == cnt
		}, 10*time.Second, 10*time.Millisecond)
		checkMetricVal(float64(cnt))
	}

	// try to async resolve 3 times
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	waitSemaphoreSizeWithCheck(3)

	// exit 1 async goroutine
	exitTask(1)
	waitSemaphoreSizeWithCheck(2)

	// try to async resolve 3 more times, semaphore is used up.
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	waitSemaphoreSizeWithCheck(5)

	// more async task will be rejected
	require.False(t, tryAsync())
	require.False(t, tryAsync())
	require.False(t, tryAsync())
	waitSemaphoreSizeWithCheck(5)
	// after some time, the metric should still be correct to test pending async tasks do not cause metric change.
	time.Sleep(10 * time.Millisecond)
	checkMetricVal(5)

	// exit a task
	exitTask(3)
	waitSemaphoreSizeWithCheck(4)

	// a new task will be accepted again
	require.True(t, tryAsync())
	waitSemaphoreSizeWithCheck(5)

	// exit all tasks
	for i := range exitLatches {
		if exitLatches[i] != nil {
			exitTask(i)
		}
	}
	waitSemaphoreSizeWithCheck(0)

	// close resolver, then all async tasks should be rejected
	lockResolver.Close()
	require.False(t, tryAsync())
	waitSemaphoreSizeWithCheck(0)
}

// TestLockResolverCloseWaitsForAsyncResolves verifies that LockResolver.Close
// blocks until any in-flight async resolve goroutines have actually exited.
// Otherwise, those goroutines could continue to use lr.store (and friends)
// after Close returns and the embedding KVStore has been torn down.
func TestLockResolverCloseWaitsForAsyncResolves(t *testing.T) {
	mockMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_close_waits_for_async_resolves_running_tasks",
		Help: "Test gauge for TestLockResolverCloseWaitsForAsyncResolves",
	})

	lockResolver := NewLockResolver(nil)

	// Block the async resolve until we explicitly release it. Track whether the
	// task has actually exited so we can detect a premature Close return.
	enterLatch := make(chan struct{})
	releaseLatch := make(chan struct{})
	var taskExited atomic.Bool

	require.True(t, lockResolver.asyncResolvePool.tryAsyncResolve(func() {
		close(enterLatch)
		<-releaseLatch
		// Simulate touching lr.store / other resolver state right before exit;
		// after Close returns, no goroutine must still be in here.
		taskExited.Store(true)
	}, mockMetric))

	// Wait until the async goroutine is definitely running.
	select {
	case <-enterLatch:
	case <-time.After(5 * time.Second):
		t.Fatal("async resolve goroutine never started")
	}

	// Run Close concurrently. It must not return until releaseLatch is closed.
	closeReturned := make(chan struct{})
	go func() {
		lockResolver.Close()
		close(closeReturned)
	}()

	// Give Close a chance to (incorrectly) return early. While the task is
	// still blocked inside resolveFn, Close MUST NOT return.
	select {
	case <-closeReturned:
		t.Fatal("LockResolver.Close returned before in-flight async resolve goroutine exited")
	case <-time.After(100 * time.Millisecond):
		// Expected: Close is still waiting.
	}

	// Release the async task; Close should now complete promptly.
	close(releaseLatch)

	select {
	case <-closeReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("LockResolver.Close did not return after async resolve goroutine exited")
	}

	require.True(t, taskExited.Load(),
		"async resolve goroutine must have completed before Close returned")
}
