package remote

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/onflow/flow-emulator/internal/mocks"
	"github.com/onflow/flow-emulator/storage"
	"github.com/onflow/flow-emulator/storage/sqlite"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestStore(t *testing.T, execClient executiondata.ExecutionDataAPIClient) *Store {
	t.Helper()

	sqliteStore, err := sqlite.New(sqlite.InMemory)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqliteStore.Close())
	})

	cache, err := lru.New[string, flowgo.RegisterValue](16)
	require.NoError(t, err)

	logger := zerolog.Nop()

	return &Store{
		Store:           sqliteStore,
		executionClient: execClient,
		forkHeight:      42,
		logger:          &logger,
		globalCache:     cache,
	}
}

func TestResolveRegisterDoesNotCacheMissingValues(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	execClient := mocks.NewMockExecutionDataAPIClient(ctrl)
	store := newTestStore(t, execClient)

	ctx := context.Background()
	id := flowgo.RegisterID{
		Owner: string([]byte{0x54, 0x91, 0x9e, 0x80, 0x9e, 0x11, 0x5e, 0x5e}),
		Key:   "$" + string([]byte{0, 0, 0, 0, 0, 0, 0, 8}),
	}
	expected := []byte{0x1, 0x2, 0x3}

	execClient.EXPECT().
		GetRegisterValues(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *executiondata.GetRegisterValuesRequest, _ ...grpc.CallOption) (*executiondata.GetRegisterValuesResponse, error) {
			require.Equal(t, store.forkHeight, req.BlockHeight)
			require.Len(t, req.RegisterIds, 1)
			require.Equal(t, []byte(id.Owner), req.RegisterIds[0].Owner)
			require.Equal(t, []byte(id.Key), req.RegisterIds[0].Key)
			return nil, status.Error(codes.NotFound, "missing")
		})

	execClient.EXPECT().
		GetRegisterValues(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *executiondata.GetRegisterValuesRequest, _ ...grpc.CallOption) (*executiondata.GetRegisterValuesResponse, error) {
			require.Equal(t, store.forkHeight, req.BlockHeight)
			require.Len(t, req.RegisterIds, 1)
			require.Equal(t, []byte(id.Owner), req.RegisterIds[0].Owner)
			require.Equal(t, []byte(id.Key), req.RegisterIds[0].Key)
			return &executiondata.GetRegisterValuesResponse{
				Values: [][]byte{expected},
			}, nil
		})

	value, err := store.resolveRegister(ctx, id, store.forkHeight)
	require.NoError(t, err)
	require.Nil(t, value)

	_, cached := store.globalCache.Get(id.String())
	require.False(t, cached)

	_, err = store.Store.GetBytesAtVersion(
		ctx,
		store.Storage(storage.LedgerStoreName),
		[]byte(id.String()),
		store.forkHeight,
	)
	require.ErrorIs(t, err, storage.ErrNotFound)

	value, err = store.resolveRegister(ctx, id, store.forkHeight)
	require.NoError(t, err)
	require.Equal(t, expected, value)

	cachedValue, cached := store.globalCache.Get(id.String())
	require.True(t, cached)
	require.Equal(t, expected, cachedValue)
}

func TestResolveRegisterDoesNotPoisonCacheOnTransientErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	execClient := mocks.NewMockExecutionDataAPIClient(ctrl)
	store := newTestStore(t, execClient)

	ctx := context.Background()
	id := flowgo.RegisterID{
		Owner: string([]byte{0x16, 0x54, 0x65, 0x33, 0x99, 0x04, 0x0a, 0x61}),
		Key:   "$" + string([]byte{0, 0, 0, 0, 0, 0, 0, 8}),
	}
	expected := []byte{0xaa, 0xbb}

	execClient.EXPECT().
		GetRegisterValues(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, status.Error(codes.DeadlineExceeded, "timeout"))

	execClient.EXPECT().
		GetRegisterValues(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&executiondata.GetRegisterValuesResponse{
			Values: [][]byte{expected},
		}, nil)

	value, err := store.resolveRegister(ctx, id, store.forkHeight)
	require.Error(t, err)
	require.Nil(t, value)

	_, cached := store.globalCache.Get(id.String())
	require.False(t, cached)

	value, err = store.resolveRegister(ctx, id, store.forkHeight)
	require.NoError(t, err)
	require.Equal(t, expected, value)
}
