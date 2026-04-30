package store

import "strings"

func normalizeHash(hash string) string {
	if hash == "" {
		return ""
	}
	return strings.ToLower(hash)
}
