package samplereader_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neilotoole/samplereader"
)

func TestSampleReader(t *testing.T) {
	b, err := ioutil.ReadFile("testdata/sample1.csv")
	require.NoError(t, err)

	br := bytes.NewReader(b)
	sr := samplereader.New(br)
	require.NoError(t, err)

	r, err := sr.NewReader()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, r.Close())
	}()

	gotB, err := ioutil.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, b, gotB)

}

func TestGenerateSample(t *testing.T) {
	generateSampleData(t)
}

func generateSampleData(t *testing.T) {
	f, err := os.Create(filepath.Join("testdata", "sample1.csv"))
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	const line = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z\n"
	for i := 0; i < 10; i++ {
		_, err = f.WriteString(strconv.Itoa(i) + "," + line)
		if err != nil {
			t.Fatal(err)
		}
	}
}
