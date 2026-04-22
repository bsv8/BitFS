package seedstorage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"

	"github.com/bsv8/BitFS/pkg/clientapp/seedcore"
)

func buildSeedV1(path string) ([]byte, string, uint32, error) {
	return seedcore.BuildSeedV1(path)
}

func parseSeedV1(seed []byte) (seedV1Meta, error) {
	meta, err := seedcore.ParseSeedV1(seed)
	if err != nil {
		return seedV1Meta{}, err
	}
	return seedV1Meta{
		FileSize:    meta.FileSize,
		ChunkCount:  meta.ChunkCount,
		ChunkHashes: meta.ChunkHashes,
		SeedHashHex: meta.SeedHashHex,
	}, nil
}

type seedV1Meta struct {
	FileSize    uint64
	ChunkCount  uint32
	ChunkHashes []string
	SeedHashHex string
}

func writeIfChanged(path string, data []byte) error {
	old, err := os.ReadFile(path)
	if err == nil && bytes.Equal(old, data) {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
