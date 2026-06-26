package v1alpha1

import (
	"testing"

	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

// The first four addresses of a prefix (network, gateway, and two reserved) are
// not allocatable: rangeStart defaults to network+4 and an explicitly-set start
// below that floor is rejected. rangeEnd defaults to the last usable address
// (broadcast-1), which is derived from the CIDR.
// Shared CIDR/host constants (altCIDR, altFloor, cidr25High, ...) live in
// effective_static_config_test.go.

func TestParseIPRange_DefaultStartIsNetworkPlus4(t *testing.T) {
	// Covers prefixes both larger and smaller than /24, and offset (non-.0) networks.
	tests := []struct {
		name      string
		cidr      string
		wantStart string
		wantEnd   string
	}{
		{"slash22", "172.16.0.0/22", "172.16.0.4", "172.16.3.254"},
		{"slash23", "10.0.0.0/23", netPlus4, altLastUsable},
		{"slash24", altCIDR, altFloor, altLastUsable},
		{"slash24_alt", testCIDR, "100.64.9.4", testHighHost},
		{"slash25_low", "10.0.1.0/25", altFloor, "10.0.1.126"},
		{"slash25_high", cidr25High, cidr25Floor, altLastUsable},
		{"slash26_low", "192.168.5.0/26", "192.168.5.4", "192.168.5.62"},
		{"slash26_offset", cidr26Off, "192.168.5.68", "192.168.5.126"},
		{"slash28", "10.0.0.0/28", netPlus4, "10.0.0.14"},
		{"slash29", "10.0.0.0/29", netPlus4, "10.0.0.6"},
		// Non-canonical input: host bits set; ParseCIDR masks to the network,
		// so the floor is computed from network 10.0.1.128.
		{"slash25_noncanonical", "10.0.1.130/25", cidr25Floor, altLastUsable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: tt.cidr}
			start, end, err := parseIPRange(cfg)
			if err != nil {
				t.Fatalf("parseIPRange(%q) unexpected error: %v", tt.cidr, err)
			}
			if start.String() != tt.wantStart {
				t.Errorf("parseIPRange(%q) start = %s, want %s", tt.cidr, start, tt.wantStart)
			}
			if end.String() != tt.wantEnd {
				t.Errorf("parseIPRange(%q) end = %s, want %s", tt.cidr, end, tt.wantEnd)
			}
		})
	}
}

func TestParseIPRange_RejectsStartBelowFloor(t *testing.T) {
	tests := []struct {
		name  string
		cidr  string
		start string
	}{
		{"slash24_network", altCIDR, "10.0.1.0"},
		{"slash24_gateway", altCIDR, "10.0.1.1"},
		{"slash24_dot2", altCIDR, "10.0.1.2"},
		{"slash24_dot3", altCIDR, "10.0.1.3"},
		{"slash25_high_below_floor", cidr25High, "10.0.1.130"},
		{"slash26_offset_below_floor", cidr26Off, "192.168.5.66"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{
				IPv4CIDR:       tt.cidr,
				IPv4RangeStart: tt.start,
			}
			if _, _, err := parseIPRange(cfg); err == nil {
				t.Errorf("parseIPRange(cidr=%q start=%q) expected error (below floor), got nil", tt.cidr, tt.start)
			}
		})
	}
}

func TestParseIPRange_AcceptsStartAtOrAboveFloor(t *testing.T) {
	tests := []struct {
		name  string
		cidr  string
		start string
	}{
		{"slash24_at_floor", altCIDR, altFloor},
		{"slash24_above_floor", altCIDR, "10.0.1.10"},
		{"slash25_high_at_floor", cidr25High, cidr25Floor},
		{"slash26_offset_at_floor", cidr26Off, "192.168.5.68"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{
				IPv4CIDR:       tt.cidr,
				IPv4RangeStart: tt.start,
			}
			start, _, err := parseIPRange(cfg)
			if err != nil {
				t.Fatalf("parseIPRange(cidr=%q start=%q) unexpected error: %v", tt.cidr, tt.start, err)
			}
			if start.String() != tt.start {
				t.Errorf("parseIPRange honored start = %s, want %s", start, tt.start)
			}
		})
	}
}

func TestParseIPRange_ExplicitEndHonored(t *testing.T) {
	cfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{
		IPv4CIDR:     altCIDR,
		IPv4RangeEnd: altMidHost,
	}
	start, end, err := parseIPRange(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if start.String() != altFloor {
		t.Errorf("start = %s, want %s (default floor)", start, altFloor)
	}
	if end.String() != altMidHost {
		t.Errorf("end = %s, want %s (explicit)", end, altMidHost)
	}
}

func TestParseIPRange_StartAfterEndErrors(t *testing.T) {
	cfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{
		IPv4CIDR:       altCIDR,
		IPv4RangeStart: "10.0.1.200",
		IPv4RangeEnd:   altMidHost,
	}
	if _, _, err := parseIPRange(cfg); err == nil {
		t.Errorf("expected error when start is after end, got nil")
	}
}

// A prefix too small to hold the network+4 floor (e.g. /30 has only .0-.3) must
// error rather than silently produce an empty/invalid range.
func TestParseIPRange_TinyCIDRErrors(t *testing.T) {
	for _, cidr := range []string{"10.0.0.0/30", "10.0.0.0/31", "10.0.0.0/32"} {
		cfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: cidr}
		if _, _, err := parseIPRange(cfg); err == nil {
			t.Errorf("parseIPRange(%q) expected error (floor exceeds usable range), got nil", cidr)
		}
	}
}

func TestParseIPRange_InvalidInputsError(t *testing.T) {
	tests := []struct {
		name string
		cfg  *vitistackcrdsv1alpha1.StaticIPAllocationConfig
	}{
		{"invalid_cidr", &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: "not-a-cidr"}},
		{"invalid_start", &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: altCIDR, IPv4RangeStart: "nope"}},
		{"invalid_end", &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: altCIDR, IPv4RangeEnd: "nope"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := parseIPRange(tt.cfg); err == nil {
				t.Errorf("parseIPRange(%+v) expected error, got nil", tt.cfg)
			}
		})
	}
}
