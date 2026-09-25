package v1alpha1

import (
	"testing"

	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

const (
	testCIDR = "100.64.9.0/24"
	altCIDR  = "10.0.1.0/24"

	// Shared host/CIDR fixtures used across the package's controller tests.
	altFloor      = "10.0.1.4"      // network+4 of altCIDR (default rangeStart floor)
	altMidHost    = "10.0.1.100"    // arbitrary mid host of altCIDR
	altLastUsable = "10.0.1.254"    // last usable host of altCIDR (also reused as a gateway fixture)
	testFirstHost = "100.64.9.1"    // first host / derived gateway of testCIDR
	testHighHost  = "100.64.9.254"  // high host of testCIDR
	cidr25High    = "10.0.1.128/25" // upper /25 of the 10.0.1.x block
	cidr25Floor   = "10.0.1.132"    // network+4 of cidr25High
	cidr26Off     = "192.168.5.64/26"
	netPlus4      = "10.0.0.4" // network+4 of any 10.0.0.0-based prefix
)

func TestFirstHost(t *testing.T) {
	tests := []struct {
		cidr string
		want string
	}{
		{testCIDR, testFirstHost},
		{altCIDR, "10.0.1.1"},
		{"192.168.5.0/26", "192.168.5.1"},
	}
	for _, tt := range tests {
		got, err := firstHost(tt.cidr)
		if err != nil {
			t.Fatalf("firstHost(%q) error: %v", tt.cidr, err)
		}
		if got != tt.want {
			t.Errorf("firstHost(%q) = %q, want %q", tt.cidr, got, tt.want)
		}
	}

	if _, err := firstHost("not-a-cidr"); err == nil {
		t.Errorf("firstHost(invalid) expected error, got nil")
	}
}

// nnWith builds a NetworkNamespace with the given spec.static and NAM status fields.
func nnWith(static *vitistackcrdsv1alpha1.StaticIPAllocationConfig, statusPrefix string, statusVlan int) *vitistackcrdsv1alpha1.NetworkNamespace {
	return &vitistackcrdsv1alpha1.NetworkNamespace{
		Spec: vitistackcrdsv1alpha1.NetworkNamespaceSpec{
			IPAllocation: &vitistackcrdsv1alpha1.NetworkNamespaceIPAllocation{
				Type:   vitistackcrdsv1alpha1.IPAllocationTypeStatic,
				Static: static,
			},
		},
		Status: vitistackcrdsv1alpha1.NetworkNamespaceStatus{
			IPv4Prefix: statusPrefix,
			VlanID:     statusVlan,
		},
	}
}

func TestEffectiveStaticConfig_FallsBackToNamStatus(t *testing.T) {
	// NAM-provisioned NN with no spec.static block.
	nn := nnWith(nil, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IPv4CIDR != testCIDR {
		t.Errorf("IPv4CIDR = %q, want 100.64.9.0/24 (from status.ipv4Prefix)", cfg.IPv4CIDR)
	}
	if cfg.IPv4Gateway != testFirstHost {
		t.Errorf("IPv4Gateway = %q, want 100.64.9.1 (first host)", cfg.IPv4Gateway)
	}
	if cfg.VlanID != 2123 {
		t.Errorf("VlanID = %d, want 2123 (from status.vlanId)", cfg.VlanID)
	}
}

func TestEffectiveStaticConfig_SpecWins(t *testing.T) {
	// Explicit spec.static overrides everything; status is ignored.
	nn := nnWith(&vitistackcrdsv1alpha1.StaticIPAllocationConfig{
		IPv4CIDR:    altCIDR,
		IPv4Gateway: altLastUsable,
		VlanID:      77,
	}, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IPv4CIDR != altCIDR || cfg.IPv4Gateway != altLastUsable || cfg.VlanID != 77 {
		t.Errorf("spec values should win, got cidr=%q gw=%q vlan=%d", cfg.IPv4CIDR, cfg.IPv4Gateway, cfg.VlanID)
	}
}

func TestEffectiveStaticConfig_PartialSpecFillsGapsFromStatus(t *testing.T) {
	// Spec sets the gateway but not the CIDR; CIDR comes from status, gateway kept.
	nn := nnWith(&vitistackcrdsv1alpha1.StaticIPAllocationConfig{
		IPv4Gateway: testHighHost,
	}, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IPv4CIDR != testCIDR {
		t.Errorf("IPv4CIDR = %q, want 100.64.9.0/24 (from status)", cfg.IPv4CIDR)
	}
	if cfg.IPv4Gateway != testHighHost {
		t.Errorf("IPv4Gateway = %q, want %s (kept from spec)", cfg.IPv4Gateway, testHighHost)
	}
}

func TestEffectiveStaticConfig_DefaultsDNSToGateway(t *testing.T) {
	// NAM-provisioned NN with no spec.static block and no DNS anywhere:
	// DNS should default to the derived gateway (first host of the CIDR).
	nn := nnWith(nil, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.DNS) != 1 || cfg.DNS[0] != testFirstHost {
		t.Errorf("DNS = %v, want [100.64.9.1] (defaulted to derived gateway)", cfg.DNS)
	}
}

func TestEffectiveStaticConfig_KeepsExplicitDNS(t *testing.T) {
	// An explicit spec.static.dns must be preserved, not overwritten by the gateway default.
	nn := nnWith(&vitistackcrdsv1alpha1.StaticIPAllocationConfig{
		DNS: []string{"8.8.8.8", "1.1.1.1"},
	}, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.DNS) != 2 || cfg.DNS[0] != "8.8.8.8" || cfg.DNS[1] != "1.1.1.1" {
		t.Errorf("DNS = %v, want [8.8.8.8 1.1.1.1] (kept from spec)", cfg.DNS)
	}
}

func TestEffectiveStaticConfig_NoCIDRAnywhereErrors(t *testing.T) {
	nn := nnWith(nil, "", 0)
	if _, err := effectiveStaticConfig(nn); err == nil {
		t.Errorf("expected error when no CIDR in spec or status, got nil")
	}
}

func TestEffectiveStaticConfig_DoesNotMutateSpec(t *testing.T) {
	spec := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{}
	nn := nnWith(spec, testCIDR, 2123)

	if _, err := effectiveStaticConfig(nn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.IPv4CIDR != "" || spec.IPv4Gateway != "" || spec.VlanID != 0 {
		t.Errorf("spec.static was mutated: %+v", spec)
	}
}
