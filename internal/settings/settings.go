// Package settings is the single source of truth for the operator's runtime
// configuration. It owns the viper instance, reads values from the environment
// (viper.AutomaticEnv), and applies defaults, so the rest of the code consumes
// typed accessors and never touches viper or os.Getenv directly.
package settings

import (
	"strconv"
	"strings"

	"github.com/spf13/viper"
	"github.com/vitistack/static-ip-operator/internal/consts"
)

// defaultMaxConcurrentReconciles is the fallback parallelism per controller
// when MAX_CONCURRENT_RECONCILES is unset, non-numeric, or less than 1.
const defaultMaxConcurrentReconciles = 5

// v is a dedicated viper instance (not the global singleton) so configuration
// state is owned here and not mutated from elsewhere. It is initialized once at
// package load; AutomaticEnv means each accessor reads the current environment.
var v = newViper()

func newViper() *viper.Viper {
	nv := viper.New()
	nv.AutomaticEnv()

	nv.SetDefault(consts.LOG_LEVEL, "info")
	nv.SetDefault(consts.LOG_JSON, false)
	nv.SetDefault(consts.LOG_ADD_CALLER, false)
	nv.SetDefault(consts.LOG_DISABLE_STACKTRACE, false)
	nv.SetDefault(consts.LOG_UNESCAPED_MULTILINE, false)
	nv.SetDefault(consts.LOG_COLORIZE_LINE, false)
	nv.SetDefault(consts.STATIC_IP_STRICT_DEFAULTS, false)
	nv.SetDefault(consts.MAX_CONCURRENT_RECONCILES, defaultMaxConcurrentReconciles)

	return nv
}

// --- Logging ---

// LogLevel is the minimum log level: "debug", "info", "warn", or "error".
func LogLevel() string { return v.GetString(consts.LOG_LEVEL) }

// LogJSON reports whether logs should be emitted as JSON.
func LogJSON() bool { return v.GetBool(consts.LOG_JSON) }

// LogAddCaller reports whether the caller (file:line) is added to log entries.
func LogAddCaller() bool { return v.GetBool(consts.LOG_ADD_CALLER) }

// LogDisableStacktrace reports whether stacktraces are suppressed.
func LogDisableStacktrace() bool { return v.GetBool(consts.LOG_DISABLE_STACKTRACE) }

// LogUnescapeMultiline reports whether multi-line messages are unescaped.
func LogUnescapeMultiline() bool { return v.GetBool(consts.LOG_UNESCAPED_MULTILINE) }

// LogColorizeLine reports whether log lines are colorized.
func LogColorizeLine() bool { return v.GetBool(consts.LOG_COLORIZE_LINE) }

// --- Controller behavior ---

// StrictDefaults reports whether the operator must refuse to fall back to
// listing NetworkNamespaces when spec.networkNamespaceName is unset. It is
// truthy for "true", "1", or "yes" (case-insensitive); anything else is false.
func StrictDefaults() bool {
	switch strings.ToLower(strings.TrimSpace(v.GetString(consts.STATIC_IP_STRICT_DEFAULTS))) {
	case "true", "1", "yes":
		return true
	default:
		return false
	}
}

// MaxConcurrentReconciles is the per-controller parallelism. A valid positive
// integer is used as-is; empty, non-numeric, zero, or negative values fall back
// to defaultMaxConcurrentReconciles.
func MaxConcurrentReconciles() int {
	n, err := strconv.Atoi(strings.TrimSpace(v.GetString(consts.MAX_CONCURRENT_RECONCILES)))
	if err != nil || n < 1 {
		return defaultMaxConcurrentReconciles
	}
	return n
}
