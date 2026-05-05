// Copyright 2025 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package async

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGo(t *testing.T) {
	l := NewRunLoop()
	n := uint32(0)

	// Go works when pool is nil (by default)
	require.Nil(t, l.Pool)
	l.Go(func() { atomic.StoreUint32(&n, 1) })
	require.Eventually(t, func() bool { return atomic.LoadUint32(&n) == 1 }, time.Second, time.Millisecond)

	// use a customized pool
	pool := &mockExecutor{}
	l.Pool = pool
	l.Go(func() { atomic.StoreUint32(&n, 2) })
	require.Equal(t, 1, len(pool.tasks))

	require.Equal(t, uint32(1), atomic.LoadUint32(&n))
	pool.tasks[0]()
	require.Equal(t, uint32(2), atomic.LoadUint32(&n))
}

func TestExecWait(t *testing.T) {
	var list []int
	l := NewRunLoop()
	time.AfterFunc(time.Millisecond, func() {
		l.Append(func() {
			list = append(list, 1)
		})
	})
	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)
}

func TestExecOnce(t *testing.T) {
	var list []int
	l := NewRunLoop()
	l.Append(func() {
		l.Append(func() {
			list = append(list, 2)
		})
		list = append(list, 1)
	})

	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 2, n)
	require.Equal(t, []int{1, 2}, list)
}

func TestExecTwice(t *testing.T) {
	var list []int
	l := NewRunLoop()
	l.Append(func() {
		time.AfterFunc(time.Millisecond, func() {
			l.Append(func() {
				list = append(list, 2)
			})
		})
		list = append(list, 1)
	})

	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)

	n, err = l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1, 2}, list)
}

func TestExecCancelWhileRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var list []int
	l := NewRunLoop()
	l.Append(
		func() {
			cancel()
			list = append(list, 1)
		},
		func() {
			list = append(list, 2)
		},
	)

	n, err := l.Exec(ctx)
	require.Error(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 1, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)
}

func TestExecCancelWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	l := NewRunLoop()
	time.AfterFunc(time.Millisecond, cancel)

	n, err := l.Exec(ctx)
	require.Error(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 0, n)
}

// TestAppendDoesNotBlockWhenExecExitsViaCtx reproduces a deadlock where
// Append flips state to Idle and then sends on the unbuffered ready channel
// after Exec has already chosen the ctx.Done() arm of its waiting select.
// Without the fix, the Append goroutine blocks forever on the send.
func TestAppendDoesNotBlockWhenExecExitsViaCtx(t *testing.T) {
	for i := 0; i < 200; i++ {
		l := NewRunLoop()
		ctx, cancel := context.WithCancel(context.Background())

		execDone := make(chan struct{})
		go func() {
			_, _ = l.Exec(ctx)
			close(execDone)
		}()

		// Wait for Exec to park in the waiting select.
		require.Eventually(t, func() bool { return l.State() == StateWaiting }, time.Second, 10*time.Microsecond)

		appendDone := make(chan struct{})
		go func() {
			// Race: cancel may cause Exec's select to pick ctx.Done() before
			// our Append send is consumed. With an unbuffered ready channel,
			// the send blocks forever in that case.
			l.Append(func() {})
			close(appendDone)
		}()

		// Cancel concurrently with the Append send to provoke the race.
		cancel()

		select {
		case <-appendDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Append blocked after ctx cancel", i)
		}
		<-execDone
	}
}

func TestExecConcurrent(t *testing.T) {
	defaultMaxProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(defaultMaxProcs)
	runtime.GOMAXPROCS(1)
	l := NewRunLoop()
	l.Append(func() {
		time.Sleep(time.Millisecond)
	})
	done := make(chan struct{})
	go func() {
		n, err := l.Exec(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, n)
		close(done)
	}()
	runtime.Gosched()
	n, err := l.Exec(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, n)
	<-done
}
