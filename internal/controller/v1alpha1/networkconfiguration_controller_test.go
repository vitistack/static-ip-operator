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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// statusPatchCounts tallies status-subresource Patch calls by target kind, so
// tests can assert that a converged reconcile performs no redundant writes.
type statusPatchCounts struct {
	nn int
	nc int
}

// countingStatusPatches returns interceptor.Funcs that count status patches by
// object kind and then delegate to the real fake-client patch.
func countingStatusPatches(c *statusPatchCounts) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourcePatch: func(
			ctx context.Context, cl client.Client, subResourceName string,
			obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			switch obj.(type) {
			case *vitistackcrdsv1alpha1.NetworkNamespace:
				c.nn++
			case *vitistackcrdsv1alpha1.NetworkConfiguration:
				c.nc++
			}
			return cl.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	}
}

// Shared test pool fixtures used across controller tests in this package.
// testGatewayIP is the first host of the prefix; testAddr1/testAddr2 are the
// first two allocatable addresses.
const (
	testIPv4Prefix = "100.64.8.0/24"
	testGatewayIP  = "100.64.8.1"
	testNamespace  = "test001"
	testAddr1      = "100.64.8.4"
	testAddr2      = "100.64.8.5"
)

// TestNCReconcile_NoRequeueWhenFullyAllocated guards against the steady-state
// 10s requeue churn: once every interface has an Allocated IPAllocation and no
// TTL is configured, the success path must NOT schedule a time-based requeue.
// The NC reacts to IPAllocation changes via the Owns() watch and to spec
// changes via the For() watch, so polling only re-lists, re-patches, and
// re-logs with nothing to do.
func TestNCReconcile_NoRequeueWhenFullyAllocated(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := vitistackcrdsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha1 to scheme: %v", err)
	}
	if err := vitistackcrdsv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha2 to scheme: %v", err)
	}

	const ns = testNamespace
	const nnName = "test-static-networknamespace"
	const ncName = "t-test-001-jnxc-ctp0"
	const ifaceName = "vlan2122"

	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: nnName, Namespace: ns},
		Spec: vitistackcrdsv1alpha1.NetworkNamespaceSpec{
			IPAllocation: &vitistackcrdsv1alpha1.NetworkNamespaceIPAllocation{
				Type: vitistackcrdsv1alpha1.IPAllocationTypeStatic,
			},
		},
		Status: vitistackcrdsv1alpha1.NetworkNamespaceStatus{
			IPv4Prefix:        testIPv4Prefix,
			VlanID:            2122,
			ProvisioningPhase: string(vitistackcrdsv1alpha2.ProvisioningPhaseReady),
		},
	}

	nc := &vitistackcrdsv1alpha1.NetworkConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:       ncName,
			Namespace:  ns,
			Finalizers: []string{finalizerName},
		},
		Spec: vitistackcrdsv1alpha1.NetworkConfigurationSpec{
			Name:                 ncName,
			NetworkNamespaceName: nnName,
			NetworkInterfaces: []vitistackcrdsv1alpha1.NetworkConfigurationInterface{
				{Name: ifaceName, Vlan: "2122"},
			},
		},
	}

	ipa := &vitistackcrdsv1alpha2.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ncName + "-" + ifaceName,
			Namespace: ns,
			Labels: map[string]string{
				vitistackcrdsv1alpha2.LabelNetworkNamespace:     nnName,
				vitistackcrdsv1alpha2.LabelNetworkConfiguration: ncName,
			},
		},
		Spec: vitistackcrdsv1alpha2.IPAllocationSpec{
			NetworkNamespaceName:     nnName,
			NetworkConfigurationName: ncName,
			InterfaceName:            ifaceName,
		},
		Status: vitistackcrdsv1alpha2.IPAllocationStatus{
			Phase:   vitistackcrdsv1alpha2.IPAllocationPhaseAllocated,
			Address: testAddr1,
			Gateway: testGatewayIP,
			Prefix:  24,
			VlanID:  2122,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn, nc, ipa).
		WithStatusSubresource(nn, nc, ipa).
		Build()

	r := &NetworkConfigurationReconciler{Client: cl, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ncName, Namespace: ns},
	})
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	if res.RequeueAfter != 0 {
		t.Errorf("fully-allocated NC with no TTL must not requeue (steady-state churn), got RequeueAfter=%v", res.RequeueAfter)
	}

	// Sanity: the success path actually ran and wrote the allocated IP to status.
	got := &vitistackcrdsv1alpha1.NetworkConfiguration{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: ncName, Namespace: ns}, got); err != nil {
		t.Fatalf("get NetworkConfiguration: %v", err)
	}
	if len(got.Status.NetworkInterfaces) != 1 || !got.Status.NetworkInterfaces[0].IPAllocated {
		t.Fatalf("expected interface to be marked IPAllocated, got %+v", got.Status.NetworkInterfaces)
	}
}

// TestNCReconcile_NNSummaryCountsOwnAllocation guards against an off-by-one in
// the NetworkNamespace ipAllocationSummary. The reconciler must count the NC's
// OWN allocation in allocatedCount, not just IPs held by *other* NCs. The bug
// derived allocatedCount from collectAllocatedIPs (which excludes self) while
// writing the AllocatedIPs list from the full IPAllocation set, so a namespace
// with N allocations reported allocatedCount = N-1 and an inconsistent summary.
func TestNCReconcile_NNSummaryCountsOwnAllocation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := vitistackcrdsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha1 to scheme: %v", err)
	}
	if err := vitistackcrdsv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha2 to scheme: %v", err)
	}

	const ns = testNamespace
	const nnName = "test-static-networknamespace"
	const ncName = "t-test-001-jnxc-ctp0"
	const ifaceName = "vlan2122"

	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: nnName, Namespace: ns},
		Spec: vitistackcrdsv1alpha1.NetworkNamespaceSpec{
			IPAllocation: &vitistackcrdsv1alpha1.NetworkNamespaceIPAllocation{
				Type: vitistackcrdsv1alpha1.IPAllocationTypeStatic,
			},
		},
		Status: vitistackcrdsv1alpha1.NetworkNamespaceStatus{
			IPv4Prefix:        testIPv4Prefix,
			VlanID:            2122,
			ProvisioningPhase: string(vitistackcrdsv1alpha2.ProvisioningPhaseReady),
		},
	}

	nc := &vitistackcrdsv1alpha1.NetworkConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:       ncName,
			Namespace:  ns,
			Finalizers: []string{finalizerName},
		},
		Spec: vitistackcrdsv1alpha1.NetworkConfigurationSpec{
			Name:                 ncName,
			NetworkNamespaceName: nnName,
			NetworkInterfaces: []vitistackcrdsv1alpha1.NetworkConfigurationInterface{
				{Name: ifaceName, Vlan: "2122"},
			},
		},
	}

	ipa := &vitistackcrdsv1alpha2.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ncName + "-" + ifaceName,
			Namespace: ns,
			Labels: map[string]string{
				vitistackcrdsv1alpha2.LabelNetworkNamespace:     nnName,
				vitistackcrdsv1alpha2.LabelNetworkConfiguration: ncName,
			},
		},
		Spec: vitistackcrdsv1alpha2.IPAllocationSpec{
			NetworkNamespaceName:     nnName,
			NetworkConfigurationName: ncName,
			InterfaceName:            ifaceName,
		},
		Status: vitistackcrdsv1alpha2.IPAllocationStatus{
			Phase:   vitistackcrdsv1alpha2.IPAllocationPhaseAllocated,
			Address: testAddr1,
			Gateway: testGatewayIP,
			Prefix:  24,
			VlanID:  2122,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn, nc, ipa).
		WithStatusSubresource(nn, nc, ipa).
		Build()

	r := &NetworkConfigurationReconciler{Client: cl, Scheme: scheme}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ncName, Namespace: ns},
	}); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	gotNN := &vitistackcrdsv1alpha1.NetworkNamespace{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: nnName, Namespace: ns}, gotNN); err != nil {
		t.Fatalf("get NetworkNamespace: %v", err)
	}

	summary := gotNN.Status.IPAllocationStatus
	if summary == nil {
		t.Fatalf("expected NetworkNamespace ipAllocationStatus to be set after reconcile")
	}

	// There is exactly one Allocated IPAllocation in the namespace (this NC's own),
	// so the summary must report allocatedCount == 1.
	if summary.AllocatedCount != 1 {
		t.Errorf("allocatedCount must include the NC's own allocation: got %d, want 1", summary.AllocatedCount)
	}

	// The count must be internally consistent with the AllocatedIPs list it is
	// written alongside, and with available = total - allocated.
	if int32(len(summary.AllocatedIPs)) != summary.AllocatedCount {
		t.Errorf("allocatedCount (%d) must match len(allocatedIPs) (%d)", summary.AllocatedCount, len(summary.AllocatedIPs))
	}
	if summary.AvailableCount != summary.TotalCount-summary.AllocatedCount {
		t.Errorf("availableCount (%d) must equal totalCount (%d) - allocatedCount (%d)",
			summary.AvailableCount, summary.TotalCount, summary.AllocatedCount)
	}
}

// stdConvergedFixture builds a NetworkNamespace, a single NetworkConfiguration
// with the finalizer already present, and that NC's Allocated IPAllocation — the
// minimal "ready to converge in one reconcile" set shared by idempotency tests.
func stdConvergedFixture() (
	*vitistackcrdsv1alpha1.NetworkNamespace,
	*vitistackcrdsv1alpha1.NetworkConfiguration,
	*vitistackcrdsv1alpha2.IPAllocation,
) {
	const ns = testNamespace
	const nnName = "test-static-networknamespace"
	const ncName = "t-test-001-jnxc-ctp0"
	const ifaceName = "vlan2122"

	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: nnName, Namespace: ns},
		Spec: vitistackcrdsv1alpha1.NetworkNamespaceSpec{
			IPAllocation: &vitistackcrdsv1alpha1.NetworkNamespaceIPAllocation{
				Type: vitistackcrdsv1alpha1.IPAllocationTypeStatic,
			},
		},
		Status: vitistackcrdsv1alpha1.NetworkNamespaceStatus{
			IPv4Prefix:        testIPv4Prefix,
			VlanID:            2122,
			ProvisioningPhase: string(vitistackcrdsv1alpha2.ProvisioningPhaseReady),
		},
	}
	nc := &vitistackcrdsv1alpha1.NetworkConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:       ncName,
			Namespace:  ns,
			Finalizers: []string{finalizerName},
		},
		Spec: vitistackcrdsv1alpha1.NetworkConfigurationSpec{
			Name:                 ncName,
			NetworkNamespaceName: nnName,
			NetworkInterfaces: []vitistackcrdsv1alpha1.NetworkConfigurationInterface{
				{Name: ifaceName, Vlan: "2122"},
			},
		},
	}
	ipa := &vitistackcrdsv1alpha2.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ncName + "-" + ifaceName,
			Namespace: ns,
			Labels: map[string]string{
				vitistackcrdsv1alpha2.LabelNetworkNamespace:     nnName,
				vitistackcrdsv1alpha2.LabelNetworkConfiguration: ncName,
			},
		},
		Spec: vitistackcrdsv1alpha2.IPAllocationSpec{
			NetworkNamespaceName:     nnName,
			NetworkConfigurationName: ncName,
			InterfaceName:            ifaceName,
		},
		Status: vitistackcrdsv1alpha2.IPAllocationStatus{
			Phase:   vitistackcrdsv1alpha2.IPAllocationPhaseAllocated,
			Address: testAddr1,
			Gateway: testGatewayIP,
			Prefix:  24,
			VlanID:  2122,
		},
	}
	return nn, nc, ipa
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := vitistackcrdsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha1 to scheme: %v", err)
	}
	if err := vitistackcrdsv1alpha2.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha2 to scheme: %v", err)
	}
	return scheme
}

// TestNCReconcile_NoStatusWriteWhenConverged guards reconciler idempotency: once
// an NC is fully allocated and the NetworkNamespace summary reflects it, a second
// reconcile with no actual change must NOT patch either the NC status or the NN
// status. Redundant patches cause apiserver round-trips, misleading "updated..."
// logs, and (at fleet scale) write churn across all NCs sharing a namespace.
func TestNCReconcile_NoStatusWriteWhenConverged(t *testing.T) {
	scheme := newTestScheme(t)
	nn, nc, ipa := stdConvergedFixture()

	counts := &statusPatchCounts{}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn, nc, ipa).
		WithStatusSubresource(nn, nc, ipa).
		WithInterceptorFuncs(countingStatusPatches(counts)).
		Build()

	r := &NetworkConfigurationReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nc.Name, Namespace: nc.Namespace}}

	// Pass 1: converge (writes are expected here).
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}

	// Pass 2: nothing changed — must be a no-op with zero status writes.
	*counts = statusPatchCounts{}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}

	if counts.nn != 0 {
		t.Errorf("converged reconcile patched NetworkNamespace status %d time(s), want 0", counts.nn)
	}
	if counts.nc != 0 {
		t.Errorf("converged reconcile patched NetworkConfiguration status %d time(s), want 0", counts.nc)
	}
}

// TestUpdateNNStatus_OrderInsensitive verifies the NN summary write is skipped
// when the same set of allocations is supplied in a different order. The cache
// List that feeds allocatedIPEntries returns map-iteration order, so without a
// deterministic sort the summary would be rewritten on every reconcile even when
// nothing changed.
func TestUpdateNNStatus_OrderInsensitive(t *testing.T) {
	scheme := newTestScheme(t)
	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: "test-static-networknamespace", Namespace: testNamespace},
	}

	counts := &statusPatchCounts{}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn).
		WithStatusSubresource(nn).
		WithInterceptorFuncs(countingStatusPatches(counts)).
		Build()

	r := &NetworkConfigurationReconciler{Client: cl, Scheme: scheme}
	staticCfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: testIPv4Prefix, VlanID: 2122}
	rangeStart := net.ParseIP(testAddr1)
	rangeEnd := net.ParseIP("100.64.8.254")

	entries := []vitistackcrdsv1alpha1.AllocatedIPEntry{
		{IP: "100.64.8.6", NetworkConfiguration: "b"},
		{IP: testAddr1, NetworkConfiguration: "a"},
		{IP: testAddr2, NetworkConfiguration: "c"},
	}

	// First write establishes the stored summary.
	if err := r.updateNetworkNamespaceIPAllocationStatus(context.Background(), nn, staticCfg, rangeStart, rangeEnd, entries); err != nil {
		t.Fatalf("first updateNetworkNamespaceIPAllocationStatus: %v", err)
	}
	if counts.nn == 0 {
		t.Fatalf("expected first call to write NN status, but it did not")
	}

	// Re-fetch so the second call compares against the persisted status.
	fresh := &vitistackcrdsv1alpha1.NetworkNamespace{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: nn.Name, Namespace: nn.Namespace}, fresh); err != nil {
		t.Fatalf("get NetworkNamespace: %v", err)
	}

	// Same set, shuffled order — must NOT trigger a rewrite.
	reordered := []vitistackcrdsv1alpha1.AllocatedIPEntry{
		{IP: testAddr2, NetworkConfiguration: "c"},
		{IP: "100.64.8.6", NetworkConfiguration: "b"},
		{IP: testAddr1, NetworkConfiguration: "a"},
	}
	*counts = statusPatchCounts{}
	if err := r.updateNetworkNamespaceIPAllocationStatus(context.Background(), fresh, staticCfg, rangeStart, rangeEnd, reordered); err != nil {
		t.Fatalf("second updateNetworkNamespaceIPAllocationStatus: %v", err)
	}
	if counts.nn != 0 {
		t.Errorf("reordered identical allocation set rewrote NN status %d time(s), want 0", counts.nn)
	}
}

// TestUpdateNNStatus_WritesWhenChanged ensures the guard does not suppress real
// changes: adding an allocation must patch the NN summary.
func TestUpdateNNStatus_WritesWhenChanged(t *testing.T) {
	scheme := newTestScheme(t)
	nn := &vitistackcrdsv1alpha1.NetworkNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: "test-static-networknamespace", Namespace: testNamespace},
	}

	counts := &statusPatchCounts{}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(nn).
		WithStatusSubresource(nn).
		WithInterceptorFuncs(countingStatusPatches(counts)).
		Build()

	r := &NetworkConfigurationReconciler{Client: cl, Scheme: scheme}
	staticCfg := &vitistackcrdsv1alpha1.StaticIPAllocationConfig{IPv4CIDR: testIPv4Prefix, VlanID: 2122}
	rangeStart := net.ParseIP(testAddr1)
	rangeEnd := net.ParseIP("100.64.8.254")

	entries := []vitistackcrdsv1alpha1.AllocatedIPEntry{
		{IP: testAddr1, NetworkConfiguration: "a"},
	}
	if err := r.updateNetworkNamespaceIPAllocationStatus(context.Background(), nn, staticCfg, rangeStart, rangeEnd, entries); err != nil {
		t.Fatalf("first updateNetworkNamespaceIPAllocationStatus: %v", err)
	}

	fresh := &vitistackcrdsv1alpha1.NetworkNamespace{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: nn.Name, Namespace: nn.Namespace}, fresh); err != nil {
		t.Fatalf("get NetworkNamespace: %v", err)
	}

	*counts = statusPatchCounts{}
	changed := []vitistackcrdsv1alpha1.AllocatedIPEntry{
		{IP: testAddr1, NetworkConfiguration: "a"},
		{IP: testAddr2, NetworkConfiguration: "b"},
	}
	if err := r.updateNetworkNamespaceIPAllocationStatus(context.Background(), fresh, staticCfg, rangeStart, rangeEnd, changed); err != nil {
		t.Fatalf("second updateNetworkNamespaceIPAllocationStatus: %v", err)
	}
	if counts.nn == 0 {
		t.Errorf("adding an allocation must rewrite NN status, but no patch occurred")
	}
}
