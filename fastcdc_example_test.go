package fastcdc_test

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"

	"github.com/lacodon/fastcdc"
)

func Example() {
	data := make([]byte, 1024*1024)
	rand.New(rand.NewSource(1337)).Read(data)

	cdc := fastcdc.New(
		bytes.NewReader(data),
		fastcdc.WithNormalSizeBytes(64*1024),
	)

	fmt.Printf("%-40s  %s\n", "sha1", "chunk size")

loop:
	for {
		chunk, err := cdc.NextChunk()
		switch {
		case errors.Is(err, io.EOF):
			break loop
		case err != nil:
			slog.Error("Failed to chunk file", slog.Any("error", err))
			return
		}

		fmt.Printf("%x  %d\n", sha1.Sum(chunk.Data), chunk.Size/1024)
	}

	// Output:
	// sha1                                      chunk size
	// 49416d02c357dad9936b577216ec3be792fa3567  64
	// 7a128165e8b4790103a10cbfd711c0fd51cb354c  96
	// abe3193e47f14b76ecef8ac1917b57fcea8de255  64
	// 0f017fac241b69567f354e4338ff3d17ec1466cb  78
	// 8023ae9d1d2936da0b0e329f4b06cd17a3393a7d  79
	// 58b88f2c99b9318a0a705036a8f30a6b6aa400f3  21
	// a956673f31c85c69e0ed695a6c75cfe0db8ecfc7  14
	// 47d44a02992a79b876638bb07cfc30c3512d21ad  99
	// be7de785ba8fc49448f2ad80d15c7abcbdd6c7c0  68
	// ec5c243791db873422f0a3c674b11a50d4df78b0  68
	// 4f752b0c703f51bf3c6363dfd14f92a954ef4749  14
	// 9a82f3355d52241e883201e638e65c63b778dd4a  65
	// afc7dbaafe30b71610eff20c40abe28743db61d1  87
	// 6cfd18da14ceb5ce51d221c553e43f56228cd01a  80
	// 50b162597b78e884161c111f34e393fe1c5bff7d  82
	// 9c1d32c14546da806c9a8287297ea985457f055b  38
}
