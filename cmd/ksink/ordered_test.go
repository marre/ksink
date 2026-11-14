package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marre/ksink/internal/format"
	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"

	"github.com/stretchr/testify/require"
)

func TestParseRotationSize(t *testing.T) {
	for _,tc:=range []struct{input string; want int64}{
		{"0",0},
		{"128MiB",128<<20},
		{"1GiB",1<<30},
		{"2KiB",2<<10},
		{"500KB",500000},
		{"42",42},
		{"3MB",3000000},
		{" 100B ",100},
	} {
		got,err:=parseRotationSize(tc.input)
		require.NoError(t,err,tc.input)
		require.Equal(t,tc.want,got,tc.input)
	}
	for _,input:=range []string{"","-1","GiB","1TiB","1.5MiB","999999999999999999999999GiB"} {
		_,err:=parseRotationSize(input)
		require.Error(t,err,input)
	}
}

func TestRunOrderedReturnsFatalOutputErrorAndDoesNotOverwrite(t *testing.T) {
	root:=t.TempDir()
	pattern:=filepath.Join(root,"{topic}","part-{seq:08}.jsonl")
	state:=filepath.Join(root,"wal")
	wal,err:=ksink.OpenTransactionLog(state)
	require.NoError(t,err)
	_,err=output.NewOrderedTopicMaterializer(wal,pattern,1,time.Second)
	require.NoError(t,err)
	_,err=wal.AppendTopicBatch([]ksink.WALTopicRecord{{Topic:"orders",Data:[]byte("acknowledged\n")}})
	require.NoError(t,err)
	require.NoError(t,wal.Close())
	conflict:=filepath.Join(root,"orders","part-00000001.jsonl")
	require.NoError(t,os.MkdirAll(filepath.Dir(conflict),0700))
	require.NoError(t,os.WriteFile(conflict,[]byte("wrong\n"),0600))
	fmtr,err:=format.New("binary",[]byte("\n"))
	require.NoError(t,err)
	// The actual CLI server must return an error, not spin forever retrying
	// the conflicting immutable output name.
	err=runOrdered("127.0.0.1:0",pattern,state,fmtr,"binary",1,time.Second)
	require.ErrorContains(t,err,"ordered output")
	raw,err:=os.ReadFile(conflict)
	require.NoError(t,err)
	require.Equal(t,"wrong\n",string(raw))
	wal,err=ksink.OpenTransactionLog(state)
	require.NoError(t,err)
	w,err:=output.NewOrderedTopicMaterializer(wal,pattern,1,time.Second)
	require.NoError(t,err)
	require.NoError(t,os.Remove(conflict))
	require.NoError(t,w.Flush(context.Background()))
	require.NoError(t,wal.Close())
	raw,err=os.ReadFile(conflict)
	require.NoError(t,err)
	require.Equal(t,"acknowledged\n",string(raw))
}
