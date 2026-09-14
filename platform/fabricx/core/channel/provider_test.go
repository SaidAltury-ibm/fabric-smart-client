/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hyperledger-labs/fabric-smart-client/pkg/utils/errors"
	fdriver "github.com/hyperledger-labs/fabric-smart-client/platform/fabric/driver"
	channelconfig "github.com/hyperledger-labs/fabric-smart-client/platform/fabricx/core/channel/config"
)

// Close owns two independent resources, so it has to release both even when the
// first one fails and report everything that went wrong. Stopping at the first
// error would leak the config monitor's goroutine.
func TestChannelCloseJoinsBothErrors(t *testing.T) {
	t.Parallel()
	inner := &stubChannel{closeErr: errors.New("inner boom")}
	// A monitor that was never started reports "monitor is not running" from
	// Stop, which gives the failing-monitor case without a live monitor.
	c := &channel{Channel: inner, Monitor: &channelconfig.ChannelConfigMonitor{}}

	err := c.Close()

	require.True(t, inner.closed, "the embedded channel must be closed even when the monitor fails")
	require.ErrorContains(t, err, "inner boom")
	require.ErrorContains(t, err, "monitor is not running")
}

func TestChannelCloseReportsMonitorErrorWhenChannelSucceeds(t *testing.T) {
	t.Parallel()
	inner := &stubChannel{}
	c := &channel{Channel: inner, Monitor: &channelconfig.ChannelConfigMonitor{}}

	err := c.Close()

	require.True(t, inner.closed)
	require.ErrorContains(t, err, "monitor is not running")
}

// stubChannel is an fdriver.Channel that records whether it was closed. Only
// Close is exercised; the rest of the interface is satisfied by the embedded
// nil interface, which panics if anything else is called by mistake.
type stubChannel struct {
	fdriver.Channel
	closed   bool
	closeErr error
}

func (c *stubChannel) Close() error {
	c.closed = true
	return c.closeErr
}
