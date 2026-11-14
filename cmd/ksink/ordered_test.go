package main

import (
	"testing"

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
