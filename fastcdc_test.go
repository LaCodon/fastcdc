package fastcdc

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestNextChunk_SmallerThanMinSize(t *testing.T) {
	t.Parallel()

	source := bytes.NewBuffer([]byte("hello world"))

	underTest := New(source)
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

	underTest := New(
		source,
		WithMinSizeBytes(1),
		WithNormalSizeBytes(6),
		WithMaxSizeBytes(6),
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

	underTest := New(
		source,
		WithMinSizeBytes(1),
		WithNormalSizeBytes(32),
		WithMaxSizeBytes(32),
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

	underTest := New(
		source,
		WithMinSizeBytes(1),
		WithNormalSizeBytes(32),
		WithMaxSizeBytes(32),
	)
	underTest.maskS = 4793662171252661683
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
