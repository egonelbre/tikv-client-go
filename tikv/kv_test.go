// Copyright 2022 TiKV Authors
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

package tikv

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestKV(t *testing.T) {
	util.EnableFailpoints()
	suite.Run(t, new(testKVSuite))
}

type testKVSuite struct {
	suite.Suite
	store          *KVStore
	cluster        *mocktikv.Cluster
	tikvStoreID    uint64
	tiflashStoreID uint64

	mockGetMinResolvedTSByStoresIDs atomic.Pointer[func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)]
}

func (s *testKVSuite) SetupTest() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	s.Require().Nil(err)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		return 0, nil, nil
	})
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0, Option(func(store *KVStore) {
		store.pdHttpClient = &mockPDHTTPClient{
			Client:                          pdhttp.NewClientWithServiceDiscovery("test", nil),
			mockGetMinResolvedTSByStoresIDs: &s.mockGetMinResolvedTSByStoresIDs,
		}
	}))
	s.Require().Nil(err)

	s.store = store
	s.cluster = cluster

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.tikvStoreID = storeIDs[0]
	s.tiflashStoreID = storeIDs[1]

	var labels []*metapb.StoreLabel
	labels = append(cluster.GetStore(s.tikvStoreID).Labels,
		&metapb.StoreLabel{Key: DCLabelKey, Value: "z1"})
	s.cluster.UpdateStorePeerAddr(s.tikvStoreID, s.storeAddr(s.tikvStoreID), labels...)
	s.store.regionCache.SetRegionCacheStore(s.tikvStoreID, s.storeAddr(s.tikvStoreID), s.storeAddr(s.tikvStoreID), tikvrpc.TiKV, 1, labels)

	labels = append(cluster.GetStore(s.tiflashStoreID).Labels,
		&metapb.StoreLabel{Key: DCLabelKey, Value: "z2"},
		&metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.cluster.UpdateStorePeerAddr(s.tiflashStoreID, s.storeAddr(s.tiflashStoreID), labels...)
	s.store.regionCache.SetRegionCacheStore(s.tiflashStoreID, s.storeAddr(s.tiflashStoreID), s.storeAddr(s.tiflashStoreID), tikvrpc.TiFlash, 1, labels)

}

func (s *testKVSuite) TearDownTest() {
	s.Require().Nil(s.store.Close())
}

func (s *testKVSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testKVSuite) setGetMinResolvedTSByStoresIDs(f func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)) {
	s.mockGetMinResolvedTSByStoresIDs.Store(&f)
}

type storeSafeTsMockClient struct {
	Client
	requestCount int32
	testSuite    *testKVSuite

	tikvSafeTs    uint64
	tiflashSafeTs uint64
}

func newStoreSafeTsMockClient(s *testKVSuite) *storeSafeTsMockClient {
	return &storeSafeTsMockClient{
		Client:        s.store.GetTiKVClient(),
		testSuite:     s,
		tikvSafeTs:    100,
		tiflashSafeTs: 80,
	}
}

func (c *storeSafeTsMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdStoreSafeTS {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	atomic.AddInt32(&c.requestCount, 1)
	resp := &tikvrpc.Response{}
	if addr == c.testSuite.storeAddr(c.testSuite.tiflashStoreID) {
		resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: c.tiflashSafeTs}
	} else {
		resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: c.tikvSafeTs}
	}
	return resp, nil
}

func (c *storeSafeTsMockClient) Close() error {
	return c.Client.Close()
}

func (c *storeSafeTsMockClient) CloseAddr(addr string) error {
	return c.Client.CloseAddr(addr)
}

type mockPDHTTPClient struct {
	pdhttp.Client
	mockGetMinResolvedTSByStoresIDs *atomic.Pointer[func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)]
}

func (c *mockPDHTTPClient) GetMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []uint64) (uint64, map[uint64]uint64, error) {
	if f := c.mockGetMinResolvedTSByStoresIDs.Load(); f != nil {
		return (*f)(ctx, storeIDs)
	}
	return c.Client.GetMinResolvedTSByStoresIDs(ctx, storeIDs)
}

func (s *testKVSuite) TestMinSafeTsFromStores() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)

	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS(oracle.GlobalTxnScope)
		s.Require().False(math.MaxUint64 == ts)
		return ts == mockClient.tiflashSafeTs
	}, 15*time.Second, time.Second)
	s.Require().GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(2))
	s.Require().Equal(mockClient.tiflashSafeTs, s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	ok, ts := s.store.getSafeTS(s.tikvStoreID)
	s.Require().True(ok)
	s.Require().Equal(mockClient.tikvSafeTs, ts)
}

func (s *testKVSuite) TestMinSafeTsFromStoresWithAllZeros() {
	// ref https://github.com/tikv/client-go/issues/1276
	mockClient := newStoreSafeTsMockClient(s)
	mockClient.tikvSafeTs = 0
	mockClient.tiflashSafeTs = 0
	s.store.SetTiKVClient(mockClient)

	s.Eventually(func() bool {
		return atomic.LoadInt32(&mockClient.requestCount) >= 4
	}, 15*time.Second, time.Second)

	s.Require().Equal(uint64(0), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromStoresWithSomeZeros() {
	// ref https://github.com/tikv/tikv/issues/13675 & https://github.com/tikv/client-go/pull/615
	mockClient := newStoreSafeTsMockClient(s)
	mockClient.tiflashSafeTs = 0
	s.store.SetTiKVClient(mockClient)

	s.Eventually(func() bool {
		return atomic.LoadInt32(&mockClient.requestCount) >= 4
	}, 15*time.Second, time.Second)

	s.Require().Equal(mockClient.tikvSafeTs, s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromPD() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		return 90, nil, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS(oracle.GlobalTxnScope)
		s.Require().False(math.MaxUint64 == ts)
		return ts == 90
	}, 15*time.Second, time.Second)
	s.Require().Equal(atomic.LoadInt32(&mockClient.requestCount), int32(0))
	s.Require().Equal(uint64(90), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromPDByStores() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		m := make(map[uint64]uint64)
		for _, id := range ids {
			m[id] = uint64(100) + id
		}
		return math.MaxUint64, m, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS(oracle.GlobalTxnScope)
		s.Require().False(math.MaxUint64 == ts)
		return ts == uint64(100)+s.tikvStoreID
	}, 15*time.Second, time.Second)
	s.Require().Equal(atomic.LoadInt32(&mockClient.requestCount), int32(0))
	s.Require().Equal(uint64(100)+s.tikvStoreID, s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromMixed1() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		m := make(map[uint64]uint64)
		for _, id := range ids {
			if id == s.tiflashStoreID {
				m[id] = 0
			} else {
				m[id] = uint64(10)
			}
		}
		return math.MaxUint64, m, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS("z1")
		s.Require().False(math.MaxUint64 == ts)
		return ts == uint64(10) && s.store.GetMinSafeTS(oracle.GlobalTxnScope) == uint64(10)
	}, 15*time.Second, time.Second)
	s.Require().GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(1))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS("z1"))
	s.Require().Equal(mockClient.tiflashSafeTs, s.store.GetMinSafeTS("z2"))
}

func (s *testKVSuite) TestMinSafeTsFromMixed2() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		m := make(map[uint64]uint64)
		for _, id := range ids {
			if id == s.tiflashStoreID {
				m[id] = uint64(10)
			} else {
				m[id] = math.MaxUint64
			}
		}
		return math.MaxUint64, m, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS("z2")
		s.Require().False(math.MaxUint64 == ts)
		return ts == uint64(10) && s.store.GetMinSafeTS(oracle.GlobalTxnScope) == uint64(10)
	}, 15*time.Second, time.Second)
	s.Require().GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(1))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	s.Require().Equal(mockClient.tikvSafeTs, s.store.GetMinSafeTS("z1"))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS("z2"))
}

func (s *testKVSuite) TestErrorHalfwayInNewKVStore() {
	// this is a leak test, TestMain will check goroutine leak
	_, err := NewKVStore("TestErrorHalfwayInNewKVStore", s.store.pdClient, NewMockSafePointKV(), &mocktikv.RPCClient{})
	require.Error(s.T(), err)
}

// uncancellableSafeTsMockClient is a Client that holds StoreSafeTS RPCs blocked
// on release and DOES NOT honor ctx cancellation. It tracks how many workers
// are in-flight. This is used to verify that KVStore.Close() waits for
// safeTSUpdater per-store goroutines to finish before returning.
type uncancellableSafeTsMockClient struct {
	Client
	release  chan struct{}
	started  chan struct{}
	startOne sync.Once
	inflight atomic.Int32
}

func (c *uncancellableSafeTsMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdStoreSafeTS {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	c.inflight.Add(1)
	defer c.inflight.Add(-1)
	c.startOne.Do(func() { close(c.started) })
	// Intentionally ignore ctx.Done() — the per-store goroutines should
	// remain alive after KVStore.Close() cancels s.ctx so we can detect
	// whether Close waited for them.
	<-c.release
	return &tikvrpc.Response{Resp: &kvrpcpb.StoreSafeTSResponse{SafeTs: 100}}, nil
}

func (c *uncancellableSafeTsMockClient) Close() error                { return c.Client.Close() }
func (c *uncancellableSafeTsMockClient) CloseAddr(addr string) error { return c.Client.CloseAddr(addr) }

// TestCloseWaitsForSafeTSUpdaterWorkers verifies that KVStore.Close() does not
// return while per-store goroutines spawned by updateSafeTS are still running.
//
// Bug: safeTSUpdater spawned one goroutine per store inside updateSafeTS that
// were tracked only by a function-local sync.WaitGroup, not by s.wg. The
// goroutine pool s.gP was also closed via a defer at the top of Close() so it
// shut down LIFO — after every other dependency (oracle, regionCache, tikv
// client, pdClient) was already torn down. As a result, in-flight per-store
// goroutines could observe torn-down clients during shutdown.
//
// Fix: register the per-store goroutines on s.wg and order Close so the
// goroutine pool drains before dependencies are released. With the fix,
// s.wg.Wait() inside Close blocks until the per-store fan-out finishes.
func TestCloseWaitsForSafeTSUpdaterWorkers(t *testing.T) {
	util.EnableFailpoints()
	tikvClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	mockGetMinResolvedTSByStoresIDs := atomic.Pointer[func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)]{}
	stub := func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		return 0, nil, nil
	}
	mockGetMinResolvedTSByStoresIDs.Store(&stub)
	store, err := NewTestTiKVStore(tikvClient, pdClient, nil, nil, 0, Option(func(s *KVStore) {
		s.pdHttpClient = &mockPDHTTPClient{
			Client:                          pdhttp.NewClientWithServiceDiscovery("test", nil),
			mockGetMinResolvedTSByStoresIDs: &mockGetMinResolvedTSByStoresIDs,
		}
	}))
	require.NoError(t, err)

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 2)
	for _, sid := range storeIDs {
		addr := fmt.Sprintf("store%d", sid)
		labels := []*metapb.StoreLabel{{Key: DCLabelKey, Value: "z1"}}
		cluster.UpdateStorePeerAddr(sid, addr, labels...)
		store.regionCache.SetRegionCacheStore(sid, addr, addr, tikvrpc.TiKV, 1, labels)
	}

	mock := &uncancellableSafeTsMockClient{
		Client:  store.GetTiKVClient(),
		release: make(chan struct{}),
		started: make(chan struct{}),
	}
	store.SetTiKVClient(mock)

	// Spawn the per-store fan-out manually so we don't have to wait for the
	// 2s safeTSUpdateInterval. Run in its own goroutine because updateSafeTS
	// itself blocks until all spawned workers complete (they're held via
	// release).
	updateDone := make(chan struct{})
	go func() {
		defer close(updateDone)
		store.updateSafeTS(context.Background())
	}()

	// Wait until at least one per-store goroutine has reached SendRequest.
	select {
	case <-mock.started:
	case <-time.After(5 * time.Second):
		close(mock.release)
		<-updateDone
		require.NoError(t, store.Close())
		t.Fatal("timed out waiting for updateSafeTS to spawn worker")
	}
	require.GreaterOrEqual(t, mock.inflight.Load(), int32(1), "expected per-store worker to be in-flight")

	// Now close the store concurrently. With the fix, Close's s.wg.Wait()
	// blocks until the per-store goroutines exit. With the bug, Close races
	// past wg.Wait() (since the workers aren't on s.wg) and tears down
	// dependencies while workers are still in-flight.
	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- store.Close()
	}()

	// Give Close a chance to advance.
	time.Sleep(100 * time.Millisecond)

	// Verify Close has NOT returned yet — there is still an in-flight worker.
	select {
	case err := <-closeReturned:
		t.Fatalf("Close returned (err=%v) while %d safeTSUpdater worker(s) still in-flight", err, mock.inflight.Load())
	default:
	}
	require.GreaterOrEqual(t, mock.inflight.Load(), int32(1), "worker exited prematurely")

	// Release the workers; updateSafeTS finishes, then Close should return.
	close(mock.release)

	select {
	case <-updateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("updateSafeTS did not return after release")
	}

	select {
	case err := <-closeReturned:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after updateSafeTS finished")
	}

	require.Equal(t, int32(0), mock.inflight.Load(), "per-store goroutines still running after Close returned")
}

func TestKVStoreCloseCheckRegionCacheClosedBeforePDClose(t *testing.T) {
	util.EnableFailpoints()
	require.NoError(t, failpoint.Enable("tikvclient/checkRegionCacheClosedBeforePDClose", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("tikvclient/checkRegionCacheClosedBeforePDClose"))
	})

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	_, _, _, _ = mocktikv.BootstrapWithMultiStores(cluster, 1)

	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	require.NoError(t, store.Close())
}
