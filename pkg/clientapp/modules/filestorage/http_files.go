package filestorage

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func handleFiles(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodGet {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		limit := 100
		offset := 0
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				limit = n
			}
		}
		if limit > 1000 {
			limit = 1000
		}
		if v := strings.TrimSpace(r.URL.Query().Get("offset")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				offset = n
			}
		}
		pathLike := strings.TrimSpace(r.URL.Query().Get("q"))
		items, total, err := dbListWorkspaceFiles(r.Context(), svc.host.Store(), limit, offset, pathLike)
		if err != nil {
			writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
			return
		}
		writeModuleOK(w, map[string]any{"total": total, "items": items})
	}
}

func handleSeeds(svc *service) moduleapi.HTTPHandler {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			writeModuleError(w, http.StatusBadRequest, "BAD_REQUEST", "request is required")
			return
		}
		if r.Method != http.MethodGet {
			writeModuleError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		items, total, err := dbListSeeds(r.Context(), svc.host.Store(), svc.host.SeedStorage(), r.URL.Query())
		if err != nil {
			writeModuleError(w, httpStatusFromErr(err), moduleapi.CodeOf(err), err.Error())
			return
		}
		writeModuleOK(w, map[string]any{"total": total, "items": items})
	}
}

func dbListSeeds(ctx context.Context, store moduleapi.Store, seedStorage moduleapi.SeedStorage, q map[string][]string) ([]map[string]any, int, error) {
	if store == nil {
		return nil, 0, nil
	}
	items := []map[string]any{}
	total := 0
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		where := ``
		args := []any{}
		if v := strings.TrimSpace(firstQueryValue(q, "seed_hash_like")); v != "" {
			where = ` WHERE seed_hash LIKE ?`
			args = append(args, "%"+v+"%")
		} else if v := strings.TrimSpace(firstQueryValue(q, "seed_hash")); v != "" {
			where = ` WHERE seed_hash = ?`
			args = append(args, strings.ToLower(v))
		}
		rows, err := rc.QueryContext(ctx, `
			SELECT DISTINCT seed_hash
			  FROM biz_workspace_files`+where+`
			 ORDER BY seed_hash ASC`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var seedHash string
			if err := rows.Scan(&seedHash); err != nil {
				return err
			}
			if seedStorage == nil {
				continue
			}
			snap, ok, err := seedStorage.LoadSeedSnapshot(ctx, seedHash)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			items = append(items, map[string]any{
				"seed_hash":             snap.SeedHash,
				"chunk_count":           snap.ChunkCount,
				"file_size":             snap.FileSize,
				"seed_file_path":        snap.SeedFilePath,
				"recommended_file_name": snap.RecommendedFileName,
				"mime_hint":             snap.MimeHint,
			})
		}
		total = len(items)
		return rows.Err()
	})
	return items, total, err
}

func firstQueryValue(q map[string][]string, key string) string {
	if len(q) == 0 {
		return ""
	}
	values := q[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func decodeJSONBody(r *http.Request, out any) error {
	if r == nil {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(out)
}
