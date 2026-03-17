/*
 * Flow Emulator
 *
 * Copyright Flow Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqlite

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	file, err := os.CreateTemp("", "test.sqlite")
	require.NoError(t, err)

	store, err := New(file.Name())
	require.NoError(t, err)

	err = store.Close()
	assert.NoError(t, err)

	_, err = New("/invalidLocation")
	assert.NotContains(
		t,
		err.Error(),
		"unable to open database file: out of memory",
		"should not attempt to open the database file if the location is invalid",
	)
	assert.ErrorContains(
		t,
		err,
		"no such file or directory",
		"should return an error indicating the location is invalid",
	)
}

func TestLoadSnapshotDoesNotMutateSnapshotFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := New(dir)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, store.Close())
	}()

	require.NoError(t, store.SetBlockHeight(1))
	require.NoError(t, store.CreateSnapshot("base"))

	require.NoError(t, store.SetBlockHeight(2))
	require.NoError(t, store.LoadSnapshot("base"))

	height, err := store.LatestBlockHeight(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), height)

	require.NoError(t, store.SetBlockHeight(3))
	require.NoError(t, store.LoadSnapshot("base"))

	height, err = store.LatestBlockHeight(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), height)
}

func TestCreateSnapshotOverwritesExistingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := New(dir)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, store.Close())
	}()

	require.NoError(t, store.SetBlockHeight(1))
	require.NoError(t, store.CreateSnapshot("base"))

	require.NoError(t, store.SetBlockHeight(2))
	require.NoError(t, store.CreateSnapshot("base"))
	require.NoError(t, store.SetBlockHeight(3))

	require.NoError(t, store.LoadSnapshot("base"))

	height, err := store.LatestBlockHeight(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(2), height)
}
