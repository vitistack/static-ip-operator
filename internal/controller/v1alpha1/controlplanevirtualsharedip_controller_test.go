package v1alpha1

import (
	"context"
	"testing"

	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newCPVIPReconciler(t *testing.T, objs ...client.Object) *ControlPlaneVirtualSharedIPReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := vitistackcrdsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP{}).
		Build()
	return &ControlPlaneVirtualSharedIPReconciler{Client: cl, Scheme: scheme}
}

func cpvipWithProvider(provider string) *vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP {
	return &vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP{
		ObjectMeta: metav1.ObjectMeta{Name: "t-test-cpvip", Namespace: "test001"},
		Spec: vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIPSpec{
			Provider:                   provider,
			NetworkNamespaceIdentifier: "test-nn",
		},
	}
}

var cpvipReq = ctrl.Request{NamespacedName: types.NamespacedName{Name: "t-test-cpvip", Namespace: "test001"}}

// Ownership is gated on CPVSharedIP.spec.provider (set by the talos-operator's
// LOADBALANCER_PROVIDER), not the NetworkNamespace's ipAllocation.provider.
// A non-"static-ip-operator" provider must be skipped at the gate — NOT requeued
// for the (deliberately absent) NetworkNamespace.
func TestCPVIPReconcile_SkipsWhenProviderNotStaticIP(t *testing.T) {
	r := newCPVIPReconciler(t, cpvipWithProvider("nam"))
	res, err := r.Reconcile(context.Background(), cpvipReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected provider=nam to be skipped at the gate (no requeue), got %+v", res)
	}
}

// A "static-ip-operator" provider is owned: it passes the gate and proceeds to
// fetch the NetworkNamespace, which is absent here, so it requeues (rather than
// skipping). That distinguishes "owned but waiting" from "not ours".
func TestCPVIPReconcile_OwnsWhenProviderStaticIP(t *testing.T) {
	r := newCPVIPReconciler(t, cpvipWithProvider(vitistackcrdsv1alpha1.ProviderNameStaticIP))
	res, err := r.Reconcile(context.Background(), cpvipReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != cpvipRequeueDelay {
		t.Errorf("expected provider=static-ip-operator to be owned and requeue %v waiting for the NN, got %+v", cpvipRequeueDelay, res)
	}
}
