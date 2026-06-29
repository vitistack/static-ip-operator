package v1alpha1

import (
	"context"
	"net"
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

	const ns = testNamespace
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
			IPv4Prefix:        testIPv4Prefix,
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
	if got.Status.Gateway != testGatewayIP {
		t.Errorf("Gateway = %q, want 100.64.8.1 (derived first host)", got.Status.Gateway)
	}
	if len(got.Status.DNS) != 1 || got.Status.DNS[0] != testGatewayIP {
		t.Errorf("DNS = %v, want [100.64.8.1] (defaulted to gateway)", got.Status.DNS)
	}
}

// TestUpdateNNSummary_SkipsUnchangedWrite verifies the IPAllocation controller's
// NN-summary writer is idempotent: re-running it with the same allocation set
// must not patch the NetworkNamespace, while a changed count must. This is the
// sibling guard to the NetworkConfiguration controller's writer — without it the
// NN summary is rewritten on every allocation/renewal even when counts are equal.
func TestUpdateNNSummary_SkipsUnchangedWrite(t *testing.T) {
	scheme := newTestScheme(t)

	const ns = testNamespace
	const nnName = "test-static-networknamespace"

	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: nnName, Namespace: ns},
	}

	counts := &statusPatchCounts{}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn).
		WithStatusSubresource(nn).
		WithInterceptorFuncs(countingStatusPatches(counts)).
		Build()

	r := &IPAllocationReconciler{Client: cl, Scheme: scheme}
	staticCfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: testIPv4Prefix, VlanID: 2122}
	rangeStart := net.ParseIP(testAddr1)
	rangeEnd := net.ParseIP("100.64.8.254")

	current := &vitistackcrdsv1alpha2.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{Name: "ipa-a", Namespace: ns},
		Status:     vitistackcrdsv1alpha2.IPAllocationStatus{Phase: vitistackcrdsv1alpha2.IPAllocationPhaseAllocated, Address: testAddr1},
	}
	existing := &vitistackcrdsv1alpha2.IPAllocationList{Items: []vitistackcrdsv1alpha2.IPAllocation{*current}}

	// First write establishes the stored summary.
	if err := r.updateNNSummary(context.Background(), nn, staticCfg, rangeStart, rangeEnd, existing, current); err != nil {
		t.Fatalf("first updateNNSummary: %v", err)
	}
	if counts.nn == 0 {
		t.Fatalf("expected first updateNNSummary to write NN status, but it did not")
	}

	// Re-fetch and re-run with the identical set — must be a no-op.
	fresh := &vitistackcrdsv1alpha1.NetworkNamespace{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: nnName, Namespace: ns}, fresh); err != nil {
		t.Fatalf("get NetworkNamespace: %v", err)
	}
	*counts = statusPatchCounts{}
	if err := r.updateNNSummary(context.Background(), fresh, staticCfg, rangeStart, rangeEnd, existing, current); err != nil {
		t.Fatalf("second updateNNSummary: %v", err)
	}
	if counts.nn != 0 {
		t.Errorf("unchanged updateNNSummary patched NN status %d time(s), want 0", counts.nn)
	}

	// A second allocation changes the count — the summary must be rewritten.
	second := vitistackcrdsv1alpha2.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{Name: "ipa-b", Namespace: ns},
		Status:     vitistackcrdsv1alpha2.IPAllocationStatus{Phase: vitistackcrdsv1alpha2.IPAllocationPhaseAllocated, Address: testAddr2},
	}
	existing.Items = append(existing.Items, second)
	*counts = statusPatchCounts{}
	if err := r.updateNNSummary(context.Background(), fresh, staticCfg, rangeStart, rangeEnd, existing, current); err != nil {
		t.Fatalf("third updateNNSummary: %v", err)
	}
	if counts.nn == 0 {
		t.Errorf("changed allocation count must rewrite NN status, but no patch occurred")
	}
}
