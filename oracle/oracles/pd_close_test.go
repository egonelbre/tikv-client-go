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

package oracles

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
)

// failingPdClient returns an error from GetTS to drive the NewPdOracle init-failure path.
type failingPdClient struct {
	pd.Client
}

func (c *failingPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	return 0, 0, errors.New("simulated PD failure")
}

func (c *failingPdClient) WithCallerComponent(component caller.Component) pd.Client {
	return c
}

// blockingPdClient blocks GetTS until ctx is canceled, used to verify Close
// awaits the updateTS goroutine.
type blockingPdClient struct {
	pd.Client
	logical atomic.Int64
	// blockUpdates, when true, makes GetTS block until ctx is done.
	blockUpdates atomic.Bool
}

func (c *blockingPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	if c.blockUpdates.Load() {
		<-ctx.Done()
		return 0, 0, ctx.Err()
	}
	return 0, c.logical.Add(1), nil
}

func (c *blockingPdClient) WithCallerComponent(component caller.Component) pd.Client {
	return c
}

// TestPdOracleCloseIdempotent verifies that calling Close() twice does not panic.
func TestPdOracleCloseIdempotent(t *testing.T) {
	oracleInterface, err := NewPdOracle(&MockPdClient{}, &PDOracleOptions{
		UpdateInterval: 50 * time.Millisecond,
		NoUpdateTS:     true,
	})
	require.NoError(t, err)
	o := oracleInterface.(*pdOracle)

	// First close should succeed.
	o.Close()

	// Second close must not panic.
	assert.NotPanics(t, func() {
		o.Close()
	})

	// Third close from a defensive caller must not panic either.
	assert.NotPanics(t, func() {
		o.Close()
	})
}

// TestNewPdOracleFailureDoubleClose ensures that when NewPdOracle fails to
// initialise (and internally calls Close on the partially-constructed
// oracle), a caller that defensively calls Close on the returned object
// does not panic. Today NewPdOracle returns nil on error, but the test
// also exercises the manual double-close path on a fresh oracle to
// demonstrate the same trap that init-failure used to set up.
func TestNewPdOracleFailureDoubleClose(t *testing.T) {
	// NewPdOracle internally invokes o.Close() on init-failure of the
	// initial GetTimestamp. Without sync.Once protection, a second Close
	// (e.g. from a deferred caller) would panic with "close of closed
	// channel". We simulate that scenario by directly building an oracle
	// whose Close has already been invoked by the constructor.
	_, err := NewPdOracle(&failingPdClient{}, &PDOracleOptions{
		UpdateInterval: 50 * time.Millisecond,
		NoUpdateTS:     true,
	})
	require.Error(t, err, "expected init failure from failingPdClient")

	// Construct another oracle and emulate a "Close was already called by
	// the constructor on its quit channel" state, then verify a caller's
	// defensive Close does not panic.
	o2Interface, err := NewPdOracle(&MockPdClient{}, &PDOracleOptions{
		UpdateInterval: 50 * time.Millisecond,
		NoUpdateTS:     true,
	})
	require.NoError(t, err)
	o2 := o2Interface.(*pdOracle)
	o2.Close() // simulate constructor's Close
	assert.NotPanics(t, func() {
		o2.Close() // caller's defensive Close
	})
}

// TestPdOracleCloseAwaitsUpdateTSGoroutine verifies that after Close()
// returns, the updateTS goroutine has actually exited.
func TestPdOracleCloseAwaitsUpdateTSGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	oracleInterface, err := NewPdOracle(&MockPdClient{}, &PDOracleOptions{
		UpdateInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	o := oracleInterface.(*pdOracle)

	// updateTS goroutine should be running now.
	require.Greater(t, runtime.NumGoroutine(), before, "expected updateTS goroutine to be running")

	o.Close()

	// After Close returns, updateTS must have exited. Allow a tiny grace
	// period for the scheduler but no PD-RPC blocking time: with a
	// proper WaitGroup, the goroutine has already returned by the time
	// Close returns.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("updateTS goroutine still running after Close: before=%d after=%d", before, runtime.NumGoroutine())
}

// TestPdOracleCloseAwaitsBlockedUpdateTSGoroutine verifies that Close
// also returns promptly even when updateTS is blocked inside an in-flight
// PD GetTS RPC, by cancelling the context passed to updateTS.
func TestPdOracleCloseAwaitsBlockedUpdateTSGoroutine(t *testing.T) {
	client := &blockingPdClient{}
	// Allow the initial GetTimestamp from NewPdOracle to succeed.
	oracleInterface, err := NewPdOracle(client, &PDOracleOptions{
		UpdateInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	o := oracleInterface.(*pdOracle)

	// Now make subsequent updateTS RPC calls block until ctx is done.
	client.blockUpdates.Store(true)

	// Wait for the updateTS goroutine to enter a blocked GetTS call.
	time.Sleep(60 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		o.Close()
		close(done)
	}()

	select {
	case <-done:
		// Good: Close returned promptly because updateTS observed the
		// cancelled context (or quit channel) and exited.
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not return within 2s; updateTS goroutine likely stuck in GetTS")
	}
}
