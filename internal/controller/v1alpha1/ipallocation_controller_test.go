package v1alpha1

import (
	"context"
	"testing"

	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	vitistackcrdsv1alpha2 "github.com/vitistack/common/pkg/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestIPAllocationReconcile_NamStaticNoStaticBlock covers the "static-via-NAM"
// case: spec.ipAllocation.static is nil, but the NAM provisioner populated
// status.ipv4Prefix. The controller must derive the pool (gateway, range, DNS)
// from the provisioned CIDR and allocate an address — not reject it for having
// "no static IP configuration".
func TestIPAllocationReconcile_NamStaticNoStaticBlock(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := vitistackcrdsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha1 to scheme: %v", err)
	}
	if err := vitistackcrdsv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha2 to scheme: %v", err)
	}

	const ns = "test001"
	const nnName = "test-static-networknamespace"

	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: nnName, Namespace: ns},
		Spec: vitistackcrdsv1alpha1.NetworkNamespaceSpec{
			IPAllocation: &vitistackcrdsv1alpha1.NetworkNamespaceIPAllocation{
				Type: vitistackcrdsv1alpha1.IPAllocationTypeStatic,
				// No Static block — this is the NAM-provisioned case.
			},
		},
		Status: vitistackcrdsv1alpha1.NetworkNamespaceStatus{
			IPv4Prefix:        "100.64.8.0/24",
			VlanID:            2122,
			ProvisioningPhase: string(vitistackcrdsv1alpha2.ProvisioningPhaseReady),
		},
	}

	ipa := &vitistackcrdsv1alpha2.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "t-test-001-jnxc-ctp0-vlan2122",
			Namespace: ns,
			Labels:    map[string]string{vitistackcrdsv1alpha2.LabelNetworkNamespace: nnName},
		},
		Spec: vitistackcrdsv1alpha2.IPAllocationSpec{
			NetworkNamespaceName: nnName,
			InterfaceName:        "vlan2122",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn, ipa).
		WithStatusSubresource(nn, ipa).
		Build()

	r := &IPAllocationReconciler{Client: cl, Scheme: scheme}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ipa.Name, Namespace: ns},
	}); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	got := &vitistackcrdsv1alpha2.IPAllocation{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: ipa.Name, Namespace: ns}, got); err != nil {
		t.Fatalf("get IPAllocation: %v", err)
	}

	if got.Status.Phase != vitistackcrdsv1alpha2.IPAllocationPhaseAllocated {
		t.Fatalf("Phase = %q (message: %q), want Allocated", got.Status.Phase, got.Status.Message)
	}
	if got.Status.Address == "" {
		t.Errorf("Address is empty, want an allocated IP from the derived range")
	}
	if got.Status.Gateway != "100.64.8.1" {
		t.Errorf("Gateway = %q, want 100.64.8.1 (derived first host)", got.Status.Gateway)
	}
	if len(got.Status.DNS) != 1 || got.Status.DNS[0] != "100.64.8.1" {
		t.Errorf("DNS = %v, want [100.64.8.1] (defaulted to gateway)", got.Status.DNS)
	}
}
