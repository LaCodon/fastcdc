package fastcdc_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/lacodon/fastcdc"
)

type errorReader struct {
	err error
}

func (e errorReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}

func TestNextChunk_SmallerThanMinSize(t *testing.T) {
	t.Parallel()

	source := bytes.NewBuffer([]byte("hello world"))

	underTest := fastcdc.New(source)
	chunk, err := underTest.NextChunk()
	requireE(t, nil, err)

	assertI(t, 0, chunk.Offset)
	assertI(t, 11, chunk.Size)
	assertB(t, []byte("hello world"), chunk.Data)

	_, err = underTest.NextChunk()
	requireE(t, io.EOF, err)
}

func TestNextChunk_CutsAtMaxSize(t *testing.T) {
	t.Parallel()

	source := bytes.NewBuffer([]byte("hello world"))

	underTest := fastcdc.New(
		source,
		fastcdc.WithMinSizeBytes(1),
		fastcdc.WithNormalSizeBytes(6),
		fastcdc.WithMaxSizeBytes(6),
	)
	chunk, err := underTest.NextChunk()
	requireE(t, nil, err)

	assertI(t, 0, chunk.Offset)
	assertI(t, 6, chunk.Size)
	assertB(t, []byte("hello "), chunk.Data)

	chunk, err = underTest.NextChunk()
	requireE(t, nil, err)

	assertI(t, 6, chunk.Offset)
	assertI(t, 5, chunk.Size)
	assertB(t, []byte("world"), chunk.Data)

	_, err = underTest.NextChunk()
	requireE(t, io.EOF, err)
}

func TestNextChunk_NoCutPoints(t *testing.T) {
	t.Parallel()

	source := bytes.NewBuffer([]byte("hello world"))

	underTest := fastcdc.New(
		source,
		fastcdc.WithMinSizeBytes(1),
		fastcdc.WithNormalSizeBytes(32),
		fastcdc.WithMaxSizeBytes(32),
	)
	chunk, err := underTest.NextChunk()
	requireE(t, nil, err)

	assertI(t, 0, chunk.Offset)
	assertI(t, 11, chunk.Size)
	assertB(t, []byte("hello world"), chunk.Data)

	_, err = underTest.NextChunk()
	requireE(t, io.EOF, err)
}

func TestNextChunk_ChunkBoundary(t *testing.T) {
	t.Parallel()

	source := bytes.NewBuffer([]byte("hello hello hello world"))

	underTest := fastcdc.New(
		source,
		fastcdc.WithMinSizeBytes(1),
		fastcdc.WithNormalSizeBytes(32),
		fastcdc.WithMaxSizeBytes(32),
		fastcdc.WithMaskS(4793662171252661683),
	)
	for i := range 3 {
		chunk, err := underTest.NextChunk()
		requireE(t, nil, err)

		assertI(t, i*6, chunk.Offset)
		assertI(t, 6, chunk.Size)
		assertB(t, []byte("hello "), chunk.Data)
	}

	chunk, err := underTest.NextChunk()
	requireE(t, nil, err)

	assertI(t, 18, chunk.Offset)
	assertI(t, 5, chunk.Size)
	assertB(t, []byte("world"), chunk.Data)

	_, err = underTest.NextChunk()
	requireE(t, io.EOF, err)
}

func TestChunks(t *testing.T) {
	t.Parallel()

	source := bytes.NewBuffer([]byte("hello hello hello "))

	underTest := fastcdc.New(
		source,
		fastcdc.WithMinSizeBytes(1),
		fastcdc.WithNormalSizeBytes(32),
		fastcdc.WithMaxSizeBytes(32),
		fastcdc.WithMaskS(4793662171252661683),
	)

	i := 0

	for chunk, err := range underTest.Chunks() {
		requireE(t, nil, err)

		assertI(t, i*6, chunk.Offset)
		assertI(t, 6, chunk.Size)
		assertB(t, []byte("hello "), chunk.Data)
		i++
	}
}

func TestReaderError(t *testing.T) {
	mockErr := errors.New("mock error")
	r := errorReader{err: mockErr}

	underTest := fastcdc.New(r)
	_, err := underTest.NextChunk()

	assertE(t, mockErr, err)
}

func TestIterReaderError(t *testing.T) {
	mockErr := errors.New("mock error")
	r := errorReader{err: mockErr}

	underTest := fastcdc.New(r)
	for _, err := range underTest.Chunks() {
		assertE(t, mockErr, err)
	}
}

func assertE(t *testing.T, expected, actual error) {
	t.Helper()
	if !errors.Is(actual, expected) {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}

func assertI(t *testing.T, expected, actual int) {
	t.Helper()
	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
	}
}

func assertB(t *testing.T, expected, actual []byte) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("\nexpected %v\ngot      %v", expected, actual)
		return
	}

	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("\nexpected %v\ngot      %v", expected, actual)
			return
		}
	}
}

func requireE(t *testing.T, expected, actual error) {
	t.Helper()
	if !errors.Is(actual, expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
