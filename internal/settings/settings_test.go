package settings

import "testing"

// clearConfigEnv blanks every config env var for the duration of a test. viper
// (with AllowEmptyEnv unset) treats an empty value as absent, so this makes the
// "no configuration" defaults deterministic regardless of the ambient shell.
func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"LOG_LEVEL", "LOG_JSON", "LOG_ADD_CALLER", "LOG_DISABLE_STACKTRACE",
		"LOG_UNESCAPED_MULTILINE", "LOG_COLORIZE_LINE",
		"STATIC_IP_STRICT_DEFAULTS", "MAX_CONCURRENT_RECONCILES",
	} {
		t.Setenv(k, "")
	}
}

func TestDefaults(t *testing.T) {
	clearConfigEnv(t)

	if got := LogLevel(); got != "info" {
		t.Errorf("LogLevel default = %q, want info", got)
	}
	if LogJSON() {
		t.Error("LogJSON default should be false")
	}
	if LogAddCaller() {
		t.Error("LogAddCaller default should be false")
	}
	if LogDisableStacktrace() {
		t.Error("LogDisableStacktrace default should be false")
	}
	if LogUnescapeMultiline() {
		t.Error("LogUnescapeMultiline default should be false")
	}
	if LogColorizeLine() {
		t.Error("LogColorizeLine default should be false")
	}
	if StrictDefaults() {
		t.Error("StrictDefaults default should be false")
	}
	if got := MaxConcurrentReconciles(); got != 5 {
		t.Errorf("MaxConcurrentReconciles default = %d, want 5", got)
	}
}

func TestLogLevelEnvOverride(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	if got := LogLevel(); got != "debug" {
		t.Errorf("LogLevel = %q, want debug", got)
	}
}

func TestLogBoolEnvOverride(t *testing.T) {
	t.Setenv("LOG_JSON", "true")
	if !LogJSON() {
		t.Error("LogJSON should be true when LOG_JSON=true")
	}
}

// TestStrictDefaultsTruthiness pins the original semantics: true for
// true/1/yes (case-insensitive), false otherwise. Note plain viper.GetBool
// would not accept "yes", which is why StrictDefaults parses explicitly.
func TestStrictDefaultsTruthiness(t *testing.T) {
	cases := map[string]bool{
		"true": true, "TRUE": true, "1": true, "yes": true, "YES": true,
		"false": false, "0": false, "no": false, "bogus": false,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			t.Setenv("STATIC_IP_STRICT_DEFAULTS", in)
			if got := StrictDefaults(); got != want {
				t.Errorf("StrictDefaults(%q) = %v, want %v", in, got, want)
			}
		})
	}
}

// TestMaxConcurrentReconciles pins the original clamp: a valid positive int is
// used as-is; empty, non-numeric, zero, or negative falls back to 5.
func TestMaxConcurrentReconciles(t *testing.T) {
	cases := map[string]int{
		"1": 1, "3": 3, "10": 10,
		"0": 5, "-2": 5, "abc": 5,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			t.Setenv("MAX_CONCURRENT_RECONCILES", in)
			if got := MaxConcurrentReconciles(); got != want {
				t.Errorf("MaxConcurrentReconciles(%q) = %d, want %d", in, got, want)
			}
		})
	}
}
