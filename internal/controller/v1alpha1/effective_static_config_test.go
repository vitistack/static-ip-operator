package v1alpha1

import (
	"testing"

	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
)

const (
	testCIDR = "100.64.9.0/24"
	altCIDR  = "10.0.1.0/24"
)

func TestFirstHost(t *testing.T) {
	tests := []struct {
		cidr string
		want string
	}{
		{testCIDR, "100.64.9.1"},
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
	if cfg.IPv4Gateway != "100.64.9.1" {
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
		IPv4Gateway: "10.0.1.254",
		VlanID:      77,
	}, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IPv4CIDR != altCIDR || cfg.IPv4Gateway != "10.0.1.254" || cfg.VlanID != 77 {
		t.Errorf("spec values should win, got cidr=%q gw=%q vlan=%d", cfg.IPv4CIDR, cfg.IPv4Gateway, cfg.VlanID)
	}
}

func TestEffectiveStaticConfig_PartialSpecFillsGapsFromStatus(t *testing.T) {
	// Spec sets the gateway but not the CIDR; CIDR comes from status, gateway kept.
	nn := nnWith(&vitistackcrdsv1alpha1.StaticIPAllocationConfig{
		IPv4Gateway: "100.64.9.254",
	}, testCIDR, 2123)

	cfg, err := effectiveStaticConfig(nn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IPv4CIDR != testCIDR {
		t.Errorf("IPv4CIDR = %q, want 100.64.9.0/24 (from status)", cfg.IPv4CIDR)
	}
	if cfg.IPv4Gateway != "100.64.9.254" {
		t.Errorf("IPv4Gateway = %q, want 100.64.9.254 (kept from spec)", cfg.IPv4Gateway)
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
