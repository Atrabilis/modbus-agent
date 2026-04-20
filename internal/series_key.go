package internal

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// BuildSeriesMetadata builds a stable per-series key and returns the filtered
// dynamic tags to persist as JSONB flags in Timescale.
//
// When no dynamic tags are present, series_key is "default" and flags is {}.
func BuildSeriesMetadata(tags map[string]string) (string, map[string]string) {
	flags := map[string]string{}
	if len(tags) == 0 {
		return "default", flags
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := strings.TrimSpace(tags[k])
		if v == "" {
			continue
		}
		if shouldSkipSeriesTagKey(k) {
			continue
		}
		flags[k] = v
	}

	if len(flags) == 0 {
		return "default", flags
	}

	canonicalKeys := make([]string, 0, len(flags))
	for k := range flags {
		canonicalKeys = append(canonicalKeys, k)
	}
	sort.Strings(canonicalKeys)

	parts := make([]string, 0, len(canonicalKeys))
	for _, k := range canonicalKeys {
		parts = append(parts, strings.ToLower(strings.TrimSpace(k))+"="+strings.TrimSpace(flags[k]))
	}

	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return "h1:" + hex.EncodeToString(sum[:]), flags
}

func shouldSkipSeriesTagKey(tagKey string) bool {
	switch strings.ToLower(strings.TrimSpace(tagKey)) {
	case "ts", "device", "device_name", "slave", "slave_name", "slave_id", "unit":
		return true
	default:
		return false
	}
}
