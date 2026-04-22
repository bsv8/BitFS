package clientapp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

func newActionID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("cmd-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("cmd-%d-%s", time.Now().UnixNano(), strings.ToLower(hex.EncodeToString(b[:])))
}

func shortToken(s string) string {
	v := strings.TrimSpace(s)
	if len(v) <= 12 {
		return v
	}
	return v[:4] + "..." + v[len(v)-4:]
}
