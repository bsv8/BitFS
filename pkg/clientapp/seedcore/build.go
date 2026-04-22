package seedcore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const SeedBlockSize = 65536

// BuildSeedV1 从文件内容生成 seed 字节、seed hash 和 chunk 数。
//
// 设计说明：
// - 这里只做纯计算，不碰数据库、不碰主干对象；
// - 上层是否写文件、是否写表，由上层决定。
func BuildSeedV1(path string) ([]byte, string, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, "", 0, err
	}
	fileSize := st.Size()
	if fileSize < 0 {
		return nil, "", 0, fmt.Errorf("negative file size")
	}
	chunkCount := uint32(ceilDiv(uint64(fileSize), SeedBlockSize))

	buf := &bytes.Buffer{}
	buf.WriteString("BSE1")
	buf.WriteByte(0x01)
	buf.WriteByte(0x01)
	_ = binary.Write(buf, binary.BigEndian, uint32(SeedBlockSize))
	_ = binary.Write(buf, binary.BigEndian, uint64(fileSize))
	_ = binary.Write(buf, binary.BigEndian, chunkCount)

	chunk := make([]byte, SeedBlockSize)
	for i := uint32(0); i < chunkCount; i++ {
		n, err := io.ReadFull(f, chunk)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, "", 0, err
			}
		}
		if n < SeedBlockSize {
			for j := n; j < SeedBlockSize; j++ {
				chunk[j] = 0
			}
		}
		h := sha256.Sum256(chunk)
		buf.Write(h[:])
	}

	seedBytes := buf.Bytes()
	h := sha256.Sum256(seedBytes)
	return seedBytes, strings.ToLower(hex.EncodeToString(h[:])), chunkCount, nil
}

// SeedV1Meta 是解析 seed 后得到的元数据。
type SeedV1Meta struct {
	FileSize    uint64
	ChunkCount  uint32
	ChunkHashes []string
	SeedHashHex string
}

// ParseSeedV1 解析 seed 文件内容。
func ParseSeedV1(seed []byte) (SeedV1Meta, error) {
	if len(seed) < 22 {
		return SeedV1Meta{}, fmt.Errorf("invalid seed length")
	}
	if string(seed[:4]) != "BSE1" {
		return SeedV1Meta{}, fmt.Errorf("invalid seed magic")
	}
	chunkSize := binary.BigEndian.Uint32(seed[6:10])
	if chunkSize != SeedBlockSize {
		return SeedV1Meta{}, fmt.Errorf("unsupported chunk size")
	}
	fileSize := binary.BigEndian.Uint64(seed[10:18])
	chunkCount := binary.BigEndian.Uint32(seed[18:22])
	expect := 22 + int(chunkCount)*32
	if len(seed) != expect {
		return SeedV1Meta{}, fmt.Errorf("invalid seed body length")
	}
	hashes := make([]string, 0, chunkCount)
	offset := 22
	for i := uint32(0); i < chunkCount; i++ {
		hashes = append(hashes, hex.EncodeToString(seed[offset:offset+32]))
		offset += 32
	}
	h := sha256.Sum256(seed)
	return SeedV1Meta{
		FileSize:    fileSize,
		ChunkCount:  chunkCount,
		ChunkHashes: hashes,
		SeedHashHex: hex.EncodeToString(h[:]),
	}, nil
}

func ceilDiv(v uint64, d uint64) uint64 {
	if v == 0 {
		return 0
	}
	return (v + d - 1) / d
}
