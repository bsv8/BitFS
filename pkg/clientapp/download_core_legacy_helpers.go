package clientapp

import (
	"fmt"
	"strings"
)

// 这组 helper 还被旧的文件浏览壳复用，所以从主下载编排文件里拆出去。
// 它们不参与 StartByHash 主线，只负责旧 UI / 兼容壳的名字提示。
func pickRecommendedFileName(quotes []DirectQuoteItem, seedHash string) string {
	defaultName := fmt.Sprintf("%s.bin", strings.ToLower(strings.TrimSpace(seedHash)))
	if len(quotes) == 0 {
		return defaultName
	}
	type fileNameStat struct {
		count    int
		firstPos int
	}
	stats := map[string]fileNameStat{}
	bestName := ""
	bestCount := -1
	bestPos := 1 << 30
	for i, q := range quotes {
		name := sanitizeRecommendedFileName(q.RecommendedFileName)
		if name == "" {
			continue
		}
		st, ok := stats[name]
		if !ok {
			st = fileNameStat{count: 0, firstPos: i}
		}
		st.count++
		stats[name] = st
		if st.count > bestCount || (st.count == bestCount && st.firstPos < bestPos) {
			bestName = name
			bestCount = st.count
			bestPos = st.firstPos
		}
	}
	if bestName != "" {
		return bestName
	}
	return defaultName
}

func pickRecommendedMIMEHint(quotes []DirectQuoteItem) string {
	if len(quotes) == 0 {
		return ""
	}
	type mimeStat struct {
		count    int
		firstPos int
	}
	stats := map[string]mimeStat{}
	bestMime := ""
	bestCount := -1
	bestPos := 1 << 30
	for i, q := range quotes {
		mimeHint := sanitizeMIMEHint(q.MimeType)
		if mimeHint == "" {
			continue
		}
		st, ok := stats[mimeHint]
		if !ok {
			st = mimeStat{count: 0, firstPos: i}
		}
		st.count++
		stats[mimeHint] = st
		if st.count > bestCount || (st.count == bestCount && st.firstPos < bestPos) {
			bestMime = mimeHint
			bestCount = st.count
			bestPos = st.firstPos
		}
	}
	return bestMime
}
