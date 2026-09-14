/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"

	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/core/generic/metrics"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/core/generic/ordering"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/core/generic/ordering/fake"
	fdriver "github.com/hyperledger-labs/fabric-smart-client/platform/fabric/driver"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/grpc"
)

// fakeListenerManager records the listeners registered against it and lets a
// test drive the finality notification by invoking the captured listener.
type fakeListenerManager struct {
	mu        sync.Mutex
	added     []fdriver.FinalityListener
	removed   []fdriver.FinalityListener
	addErr    error
	removeErr error
}

func (m *fakeListenerManager) AddFinalityListener(_ string, listener fdriver.FinalityListener) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addErr != nil {
		return m.addErr
	}
	m.added = append(m.added, listener)
	return nil
}

func (m *fakeListenerManager) RemoveFinalityListener(_ string, listener fdriver.FinalityListener) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removeErr != nil {
		return m.removeErr
	}
	m.removed = append(m.removed, listener)
	return nil
}

// listener returns the single listener registered so far, waiting briefly for
// IsFinal's registration to land since it runs on the caller's goroutine only
// up to the point where it blocks.
func (m *fakeListenerManager) listener(t *testing.T) fdriver.FinalityListener {
	t.Helper()
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.added) == 1
	}, time.Second, time.Millisecond, "listener was never registered")

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.added[0]
}

func (m *fakeListenerManager) removedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.removed)
}

// runIsFinal calls IsFinal on a separate goroutine and reports the status to
// the returned listener. IsFinal blocks until the listener fires or the context
// is cancelled, so the notification has to come from outside the call.
func runIsFinal(t *testing.T, ctx context.Context, m *fakeListenerManager, txID string) <-chan error {
	t.Helper()
	adapter := &finalityServiceAdapter{manager: m}
	errCh := make(chan error, 1)
	go func() { errCh <- adapter.IsFinal(ctx, txID) }()
	return errCh
}

func awaitErr(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("IsFinal did not return")
		return nil
	}
}

func TestFinalityServiceAdapterIsFinalValid(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{}
	errCh := runIsFinal(t, context.Background(), m, "tx1")

	m.listener(t).OnStatus(context.Background(), "tx1", fdriver.Valid, "")

	require.NoError(t, awaitErr(t, errCh))
	// The listener is always removed, so a completed IsFinal leaves no
	// registration behind on the manager.
	require.Equal(t, 1, m.removedCount())
}

func TestFinalityServiceAdapterIsFinalNonValidStatuses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		msg    string
		expect string
	}{
		{"invalid", fdriver.Invalid, "bad tx", "is invalid: bad tx"},
		{"unknown", fdriver.Unknown, "timed out", "status is unknown: timed out"},
		{"unexpected", fdriver.Busy, "still busy", "has unexpected status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := &fakeListenerManager{}
			errCh := runIsFinal(t, context.Background(), m, "tx1")

			m.listener(t).OnStatus(context.Background(), "tx1", tc.status, tc.msg)

			err := awaitErr(t, errCh)
			require.ErrorContains(t, err, tc.expect)
			require.ErrorContains(t, err, "tx1")
			require.Equal(t, 1, m.removedCount())
		})
	}
}

func TestFinalityServiceAdapterIsFinalAddListenerError(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{addErr: context.DeadlineExceeded}
	adapter := &finalityServiceAdapter{manager: m}

	err := adapter.IsFinal(context.Background(), "tx1")

	require.ErrorContains(t, err, "failed to add finality listener")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// Registration failed, so there is nothing to clean up.
	require.Zero(t, m.removedCount())
}

func TestFinalityServiceAdapterIsFinalContextCancelled(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := runIsFinal(t, ctx, m, "tx1")

	// Make sure the listener is registered before cancelling, so the
	// cancellation races against a real wait rather than the setup.
	m.listener(t)
	cancel()

	err := awaitErr(t, errCh)
	require.ErrorContains(t, err, "context cancelled while waiting")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, m.removedCount())
}

// A failing RemoveFinalityListener is logged, not surfaced: the transaction
// already reached finality and the caller's result must not change.
func TestFinalityServiceAdapterIsFinalRemoveErrorIgnored(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{removeErr: context.DeadlineExceeded}
	errCh := runIsFinal(t, context.Background(), m, "tx1")

	m.listener(t).OnStatus(context.Background(), "tx1", fdriver.Valid, "")

	require.NoError(t, awaitErr(t, errCh))
}

// OnStatus is invoked once per registration by the manager, but the adapter
// buffers a single result and drops the rest, so a duplicate notification must
// not block the notifier or change the outcome.
func TestFinalityServiceAdapterIsFinalDuplicateStatusIgnored(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{}
	errCh := runIsFinal(t, context.Background(), m, "tx1")

	l := m.listener(t)
	l.OnStatus(context.Background(), "tx1", fdriver.Valid, "")
	// The second call must return rather than block on the full channel.
	l.OnStatus(context.Background(), "tx1", fdriver.Invalid, "ignored")

	require.NoError(t, awaitErr(t, errCh))
}

func TestFinalityListenerOnStatus(t *testing.T) {
	t.Parallel()
	var gotTxID, gotMsg string
	var gotStatus int
	l := &finalityListener{
		onStatusFunc: func(_ context.Context, txID string, status int, statusMessage string) {
			gotTxID, gotStatus, gotMsg = txID, status, statusMessage
		},
	}

	l.OnStatus(context.Background(), "tx7", fdriver.Valid, "committed")

	require.Equal(t, "tx7", gotTxID)
	require.Equal(t, fdriver.Valid, gotStatus)
	require.Equal(t, "committed", gotMsg)
}

// fakeVault stands in for the delivery vault during channel construction and
// reports an empty ledger, so delivery starts from the beginning.
func TestFakeVault(t *testing.T) {
	t.Parallel()
	v := &fakeVault{}

	txID, err := v.GetLastTxID(context.Background())
	require.NoError(t, err)
	require.Empty(t, txID)

	block, err := v.GetLastBlock(context.Background())
	require.NoError(t, err)
	require.Zero(t, block)
}

// The adapter reaches the concrete *ordering.Service to call Configure, so any
// other Ordering implementation is rejected rather than silently ignored.
func TestOrderingServiceAdapterConfigureRejectsForeignService(t *testing.T) {
	t.Parallel()
	a := &orderingServiceAdapter{os: &stubOrdering{}}

	err := a.Configure(ordering.BFT, []*grpc.ConnectionConfig{})

	require.ErrorContains(t, err, "ordering service is not an")
}

// newOrderingService builds a real *ordering.Service, which is what the adapter
// type-asserts to. Only the BFT broadcaster is registered, so a consensus type
// that fails to map onto BFT cannot be selected and Configure reports an error.
func newOrderingService(t *testing.T) (*ordering.Service, *fake.ConfigService) {
	t.Helper()
	cs := &fake.ConfigService{PoolSizeValue: 1, RetriesValue: 1, NetworkNameValue: "test-network"}
	s := ordering.NewService(
		func(string) (fdriver.EndorserTransactionService, error) { return nil, nil },
		nil,
		cs,
		&metrics.Metrics{},
		nil,
	)
	s.Broadcasters = map[ordering.ConsensusType]ordering.BroadcastFnc{
		ordering.BFT: func(context.Context, *common.Envelope) error { return nil },
	}
	return s, cs
}

// "arma" is the consensus type fabric-x-orderer writes into genesis blocks.
// The adapter translates it to BFT, because that is the broadcaster the
// ordering service actually registers; without the translation the channel
// cannot be configured at all.
func TestOrderingServiceAdapterConfigureMapsArmaToBFT(t *testing.T) {
	t.Parallel()
	s, cs := newOrderingService(t)
	a := &orderingServiceAdapter{os: s}
	orderers := []*grpc.ConnectionConfig{{Address: "orderer:7050"}}

	require.NoError(t, a.Configure(armaConsensusType, orderers))
	require.Equal(t, orderers, cs.OrderersValue, "orderers must reach the config service")
}

// A consensus type the ordering service does have a broadcaster for is passed
// through untouched.
func TestOrderingServiceAdapterConfigurePassesThroughKnownType(t *testing.T) {
	t.Parallel()
	s, cs := newOrderingService(t)
	a := &orderingServiceAdapter{os: s}
	orderers := []*grpc.ConnectionConfig{{Address: "orderer:7050"}}

	require.NoError(t, a.Configure(ordering.BFT, orderers))
	require.Equal(t, orderers, cs.OrderersValue)
}

// An unmappable consensus type has no registered broadcaster, so the failure
// surfaces instead of leaving the service silently unconfigured.
func TestOrderingServiceAdapterConfigureUnknownType(t *testing.T) {
	t.Parallel()
	s, cs := newOrderingService(t)
	a := &orderingServiceAdapter{os: s}

	err := a.Configure("no-such-consensus", []*grpc.ConnectionConfig{{Address: "orderer:7050"}})

	require.ErrorContains(t, err, "failed to set consensus type")
	require.Nil(t, cs.OrderersValue, "orderers must not be applied when consensus selection fails")
}

func TestCommitterServiceIsFinalDelegates(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{}
	adapter := &finalityServiceAdapter{manager: m}
	c := &committerService{finalityService: adapter}

	errCh := make(chan error, 1)
	go func() { errCh <- c.IsFinal(context.Background(), "tx1") }()
	m.listener(t).OnStatus(context.Background(), "tx1", fdriver.Valid, "")

	require.NoError(t, awaitErr(t, errCh))
}

// Without a finality service there is nothing to wait on, so IsFinal reports
// success rather than failing the caller.
func TestCommitterServiceIsFinalWithoutFinalityService(t *testing.T) {
	t.Parallel()
	c := &committerService{}
	require.NoError(t, c.IsFinal(context.Background(), "tx1"))
}

// The finality listener calls reach the underlying manager only through a
// *finalityServiceAdapter; any other fdriver.Finality is a no-op.
func TestCommitterServiceFinalityListenersReachManager(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{}
	c := &committerService{finalityService: &finalityServiceAdapter{manager: m}}
	l := &finalityListener{onStatusFunc: func(context.Context, string, int, string) {}}

	require.NoError(t, c.AddFinalityListener("tx1", l))
	require.NoError(t, c.RemoveFinalityListener("tx1", l))

	m.mu.Lock()
	defer m.mu.Unlock()
	require.Len(t, m.added, 1)
	require.Len(t, m.removed, 1)
}

func TestCommitterServiceFinalityListenersWithoutAdapter(t *testing.T) {
	t.Parallel()
	l := &finalityListener{onStatusFunc: func(context.Context, string, int, string) {}}

	// No finality service at all.
	c := &committerService{}
	require.NoError(t, c.AddFinalityListener("tx1", l))
	require.NoError(t, c.RemoveFinalityListener("tx1", l))

	// A finality service that is not a *finalityServiceAdapter.
	c = &committerService{finalityService: &stubFinality{}}
	require.NoError(t, c.AddFinalityListener("tx1", l))
	require.NoError(t, c.RemoveFinalityListener("tx1", l))
}

func TestCommitterServiceFinalityListenerErrorsPropagate(t *testing.T) {
	t.Parallel()
	m := &fakeListenerManager{addErr: context.DeadlineExceeded, removeErr: context.Canceled}
	c := &committerService{finalityService: &finalityServiceAdapter{manager: m}}
	l := &finalityListener{onStatusFunc: func(context.Context, string, int, string) {}}

	require.ErrorIs(t, c.AddFinalityListener("tx1", l), context.DeadlineExceeded)
	require.ErrorIs(t, c.RemoveFinalityListener("tx1", l), context.Canceled)
}

// Fabric-x drives commit and validation through the committer sidecar rather
// than this client, so these members exist only to satisfy driver.Committer.
// They are asserted to stay inert: a future implementation that starts doing
// work here has to update this test deliberately.
func TestCommitterServiceUnimplementedMembersAreInert(t *testing.T) {
	t.Parallel()
	c := &committerService{}

	require.NoError(t, c.ReloadConfigTransactions())
	require.NoError(t, c.Commit(context.Background(), nil))
	require.NoError(t, c.Start(context.Background()))
	require.NoError(t, c.ProcessNamespace())
	require.NoError(t, c.ProcessNamespace("ns1", "ns2"))
	require.NoError(t, c.AddTransactionFilter(nil))
	require.NoError(t, c.DiscardTx(context.Background(), "tx1", "because"))
	require.NoError(t, c.CommitTX(context.Background(), "tx1", 1, 0, nil))

	code, msg, err := c.Status(context.Background(), "tx1")
	require.NoError(t, err)
	require.Zero(t, code)
	require.Empty(t, msg)
}

// Delivery is driven by the committer's notification stream, so the embedded
// delivery service must not be started by the channel.
func TestNoopDeliveryServiceStartDoesNotStartEmbedded(t *testing.T) {
	t.Parallel()
	inner := &stubDeliveryService{}
	d := &noopDeliveryService{DeliveryService: inner}

	require.NoError(t, d.Start(context.Background()))
	require.False(t, inner.started, "embedded delivery service must not be started")
}

// --- stubs -------------------------------------------------------------------

// stubOrdering is an fdriver.Ordering that is deliberately not an
// *ordering.Service, to exercise the adapter's type check.
type stubOrdering struct{}

func (*stubOrdering) Broadcast(context.Context, any) error { return nil }

func (*stubOrdering) SetConsensusType(string) error { return nil }

// stubFinality is an fdriver.Finality that is not a *finalityServiceAdapter.
type stubFinality struct{}

func (*stubFinality) IsFinal(context.Context, string) error { return nil }

// stubDeliveryService is a generic.DeliveryService that records whether it was
// started, so the noop wrapper can be shown not to start it.
type stubDeliveryService struct {
	started bool
}

func (s *stubDeliveryService) Start(context.Context) error {
	s.started = true
	return nil
}

func (*stubDeliveryService) Stop() {}

func (*stubDeliveryService) ScanBlock(context.Context, fdriver.BlockCallback) error { return nil }

func (*stubDeliveryService) ScanBlockFrom(context.Context, fdriver.BlockNum, fdriver.BlockCallback) error {
	return nil
}

func (*stubDeliveryService) Scan(context.Context, fdriver.TxID, fdriver.DeliveryCallback) error {
	return nil
}

func (*stubDeliveryService) ScanFromBlock(context.Context, fdriver.BlockNum, fdriver.DeliveryCallback) error {
	return nil
}
