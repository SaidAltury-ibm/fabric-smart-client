/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mockviewmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hyperledger-labs/fabric-smart-client/integration/benchmark"
	benchviews "github.com/hyperledger-labs/fabric-smart-client/integration/benchmark/views"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/view"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkDirect(b *testing.B) {
	benchmarks := []struct {
		name   string
		newView func(tb testing.TB) view.View
	}{
		{
			name: "noop",
			newView: func(tb testing.TB) view.View {
				factory := &benchviews.NoopViewFactory{}
				v, err := factory.NewView(nil)
				require.NoError(tb, err)
				return v
			},
		},
		{
			name: "cpu",
			newView: func(tb testing.TB) view.View {
				params := &benchviews.CPUParams{N: 200000}
				input, err := json.Marshal(params)
				require.NoError(tb, err)
				factory := &benchviews.CPUViewFactory{}
				v, err := factory.NewView(input)
				require.NoError(tb, err)
				return v
			},
		},
		{
			name: "ecdsa",
			newView: func(tb testing.TB) view.View {
				params := &benchviews.ECDSASignParams{}
				input, err := json.Marshal(params)
				require.NoError(tb, err)
				factory := &benchviews.ECDSASignViewFactory{}
				v, err := factory.NewView(input)
				require.NoError(tb, err)
				return v
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("mockManager/%s", bm.name), func(b *testing.B) {
			v := bm.newView(b)

			vm := &benchmark.MockViewManager{
				Constructor: func() view.View { return v },
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				f, err := vm.NewView(bm.name, nil)
				assert.NoError(b, err)

				for pb.Next() {
					_, err := vm.InitiateView(f, context.Background())
					assert.NoError(b, err)
				}
			})
			benchmark.ReportTPS(b)
		})
	}
}