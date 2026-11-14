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

func TestWALFormatterBindingSurvivesCompactionAndRejectsChangedConfiguration(t *testing.T) {
	dir:=filepath.Join(t.TempDir(),"state")
	wal,err:=OpenTransactionLog(dir)
	require.NoError(t,err)
	require.NoError(t,wal.BindOutput(filepath.Join(dir,"output","{topic}","messages-{seq:08}.jsonl")))
	require.NoError(t,wal.BindFormatter("ordered-v1|jsonl|key-utf8"))
	_,err=wal.AppendTopicBatch([]WALTopicRecord{{Topic:"orders",Data:[]byte("record\n")}})
	require.NoError(t,err)
	require.NoError(t,wal.Compact())
	require.NoError(t,wal.Close())

	reopened,err:=OpenTransactionLog(dir)
	require.NoError(t,err)
	defer func(){_=reopened.Close()}()
	require.NoError(t,reopened.BindFormatter("ordered-v1|jsonl|key-utf8"))
	require.ErrorContains(t,reopened.BindFormatter("ordered-v1|binary|raw"),
		"WAL output format changed")
	require.NoError(t,reopened.BindFormatter("ordered-v1|jsonl|key-utf8"),
		"rejecting an invalid change must not corrupt the WAL")
	require.Equal(t,int64(1),reopened.NextTopicOffset("orders"))
	record,exists,err:=reopened.ReadTopicRecord("orders",0)
	require.NoError(t,err)
	require.True(t,exists)
	require.Equal(t,"record\n",string(record.Data))
}
