package seedstorage

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

// SeedDirPath 计算 `<config_dir>/<publickey_hex>/seeds/`。
//
// 设计说明：
// - configPath 必须是配置文件路径，不接受目录；
// - publickeyHex 必须已经是系统内唯一身份；
// - 这里不做宽松兜底，缺一个就直接报错，避免 seed 落错目录。
func SeedDirPath(configPath, publickeyHex string) (string, error) {
	configPath = strings.TrimSpace(configPath)
	publickeyHex = strings.ToLower(strings.TrimSpace(publickeyHex))
	if configPath == "" {
		return "", fmt.Errorf("config path is required")
	}
	if publickeyHex == "" {
		return "", fmt.Errorf("public key hex is required")
	}
	if _, err := hex.DecodeString(publickeyHex); err != nil {
		return "", fmt.Errorf("public key hex is invalid")
	}
	configDir := filepath.Clean(filepath.Dir(configPath))
	if configDir == "." || configDir == string(filepath.Separator) {
		return "", fmt.Errorf("config directory is required")
	}
	return filepath.Join(configDir, publickeyHex, "seeds"), nil
}

func SeedFilePath(configPath, publickeyHex, seedHash string) (string, error) {
	dir, err := SeedDirPath(configPath, publickeyHex)
	if err != nil {
		return "", err
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if len(seedHash) != 64 {
		return "", fmt.Errorf("seed hash is required")
	}
	if _, err := hex.DecodeString(seedHash); err != nil {
		return "", fmt.Errorf("seed hash is invalid")
	}
	return filepath.Join(dir, seedHash+".bse"), nil
}
