package ksink

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWALRejectsPreviousUnmergedManifestVersionWithoutReset(t *testing.T) {
	dir:=filepath.Join(t.TempDir(),"state")
	wal,err:=OpenTransactionLog(dir)
	require.NoError(t,err)
	require.NoError(t,wal.Close())
	path:=filepath.Join(dir,"manifest.json")
	raw,err:=os.ReadFile(path)
	require.NoError(t,err)
	var manifest walManifest
	require.NoError(t,json.Unmarshal(raw,&manifest))
	manifest.Version=walManifestVersion-1
	older,err:=json.Marshal(manifest)
	require.NoError(t,err)
	require.NoError(t,os.WriteFile(path,older,0600))
	_,err=OpenTransactionLog(dir)
	require.ErrorContains(t,err,"unsupported transaction WAL manifest version")
	contents,err:=os.ReadFile(path)
	require.NoError(t,err)
	require.Equal(t,older,contents,"startup must never auto-migrate or reset acknowledged history")
}
