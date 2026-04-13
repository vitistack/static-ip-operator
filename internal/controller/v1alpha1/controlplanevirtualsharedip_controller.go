/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	viticommonconditions "github.com/vitistack/common/pkg/operator/conditions"
	viticommonfinalizers "github.com/vitistack/common/pkg/operator/finalizers"
	reconcileutil "github.com/vitistack/common/pkg/operator/reconcileutil"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	cpvipFinalizerName = "vitistack.io/static-ip-cpvip-finalizer"
	cpvipRequeueDelay  = 10 * time.Second
)

// ControlPlaneVirtualSharedIPReconciler reconciles ControlPlaneVirtualSharedIP
// resources for NetworkNamespaces that use static IP allocation. It allocates
// a VIP IP from the static pool via a child NetworkConfiguration and populates
// the CPVIP status with LoadBalancerIps.
type ControlPlaneVirtualSharedIPReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=vitistack.io,resources=controlplanevirtualsharedips,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=controlplanevirtualsharedips/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=controlplanevirtualsharedips/finalizers,verbs=update
// +kubebuilder:rbac:groups=vitistack.io,resources=networkconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=networknamespaces,verbs=get;list;watch

func (r *ControlPlaneVirtualSharedIPReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cpvip := &vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP{}
	if err := r.Get(ctx, req.NamespacedName, cpvip); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("reconciling ControlPlaneVirtualSharedIP",
		"name", cpvip.Name, "namespace", req.Namespace, "generation", cpvip.GetGeneration())

	// Look up the referenced NetworkNamespace
	nnName := strings.TrimSpace(cpvip.Spec.NetworkNamespaceIdentifier)
	if nnName == "" {
		log.Info("no networkNamespaceIdentifier set, skipping")
		return ctrl.Result{}, nil
	}

	nn := &vitistackcrdsv1alpha1.NetworkNamespace{}
	if err := r.Get(ctx, client.ObjectKey{Name: nnName, Namespace: req.Namespace}, nn); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("referenced NetworkNamespace not found, requeuing", "networkNamespace", nnName)
			return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
		}
		return ctrl.Result{}, err
	}

	// Provider check: only handle if NN uses static-ip-operator.
	// This runs before adding a finalizer to avoid claiming resources
	// that belong to the nms-operator.
	if nn.Spec.IPAllocation == nil ||
		nn.Spec.IPAllocation.Type != vitistackcrdsv1alpha1.IPAllocationTypeStatic ||
		!vitistackcrdsv1alpha1.MatchesProvider(nn.Spec.IPAllocation.Provider, vitistackcrdsv1alpha1.ProviderNameStaticIP) {
		log.V(1).Info("NetworkNamespace does not use static-ip-operator, skipping",
			"networkNamespace", nnName)
		return ctrl.Result{}, nil
	}

	// Handle deletion
	if !cpvip.GetDeletionTimestamp().IsZero() {
		return r.handleDeletion(ctx, cpvip, log)
	}

	// Ensure finalizer
	if !viticommonfinalizers.Has(cpvip, cpvipFinalizerName) {
		if err := viticommonfinalizers.Ensure(ctx, r.Client, cpvip, cpvipFinalizerName); err != nil {
			log.Error(err, "failed to ensure finalizer")
			return reconcileutil.Requeue(err)
		}
		log.Info("added finalizer to ControlPlaneVirtualSharedIP")
		return ctrl.Result{}, nil
	}

	// Ensure child NetworkConfiguration exists for VIP allocation
	vipNCName := cpvip.Spec.ClusterIdentifier + "-cpvip"
	vipNC, err := r.ensureVIPNetworkConfiguration(ctx, cpvip, nn, vipNCName, log)
	if err != nil {
		log.Error(err, "failed to ensure VIP NetworkConfiguration")
		r.setCPVIPStatus(ctx, cpvip, "Error", "Failed",
			fmt.Sprintf("failed to ensure VIP NetworkConfiguration: %v", err),
			nil, cpvip.GetGeneration())
		return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
	}

	// Check if the NC has an error
	if vipNC.Status.Phase == "Error" {
		msg := fmt.Sprintf("VIP NetworkConfiguration %s is in error state: %s", vipNCName, vipNC.Status.Message)
		log.Info(msg)
		r.setCPVIPStatus(ctx, cpvip, "Error", "IPAllocationFailed", msg, nil, cpvip.GetGeneration())
		return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
	}

	// Read allocated IP from NC status
	vipIP := extractAllocatedVIPIP(vipNC)
	if vipIP == "" {
		log.Info("VIP NetworkConfiguration does not have allocated IP yet, requeuing",
			"ncName", vipNCName, "ncPhase", vipNC.Status.Phase)
		r.setCPVIPStatus(ctx, cpvip, "Pending", "WaitingForIPAllocation",
			fmt.Sprintf("Waiting for IP allocation on NetworkConfiguration %s", vipNCName),
			nil, 0)
		return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
	}

	// Success: set CPVIP status with LoadBalancerIps
	log.Info("VIP IP allocated", "ip", vipIP, "cluster", cpvip.Spec.ClusterIdentifier)
	r.setCPVIPStatus(ctx, cpvip, "Ready", "Synced",
		fmt.Sprintf("VIP allocated: %s", vipIP),
		[]string{vipIP}, cpvip.GetGeneration())

	return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
}

// ensureVIPNetworkConfiguration creates or gets the child NetworkConfiguration for VIP allocation.
func (r *ControlPlaneVirtualSharedIPReconciler) ensureVIPNetworkConfiguration(
	ctx context.Context,
	cpvip *vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP,
	nn *vitistackcrdsv1alpha1.NetworkNamespace,
	ncName string,
	log logr.Logger,
) (*vitistackcrdsv1alpha1.NetworkConfiguration, error) {
	nc := &vitistackcrdsv1alpha1.NetworkConfiguration{}
	err := r.Get(ctx, client.ObjectKey{Name: ncName, Namespace: cpvip.Namespace}, nc)
	if err == nil {
		return nc, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	nc = &vitistackcrdsv1alpha1.NetworkConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ncName,
			Namespace: cpvip.Namespace,
			Labels: map[string]string{
				vitistackcrdsv1alpha1.ClusterIdAnnotation: cpvip.Spec.ClusterIdentifier,
				"vitistack.io/purpose":                    "control-plane-vip",
			},
		},
		Spec: vitistackcrdsv1alpha1.NetworkConfigurationSpec{
			Name:                 ncName,
			NetworkNamespaceName: nn.Name,
			Provider:             vitistackcrdsv1alpha1.ProviderNameStaticIP,
			NetworkInterfaces: []vitistackcrdsv1alpha1.NetworkConfigurationInterface{
				{Name: "vip"},
			},
		},
	}

	// Set ownerReference so Owns() watch works and for GC
	if err := ctrl.SetControllerReference(cpvip, nc, r.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	if err := r.Create(ctx, nc); err != nil {
		return nil, fmt.Errorf("failed to create VIP NetworkConfiguration: %w", err)
	}

	log.Info("created VIP NetworkConfiguration", "name", ncName)
	return nc, nil
}

// extractAllocatedVIPIP reads the allocated VIP IP from the NetworkConfiguration status.
func extractAllocatedVIPIP(nc *vitistackcrdsv1alpha1.NetworkConfiguration) string {
	for _, iface := range nc.Status.NetworkInterfaces {
		if iface.Name == "vip" && iface.IPAllocated && len(iface.IPv4Addresses) > 0 {
			return iface.IPv4Addresses[0]
		}
	}
	return ""
}

// handleDeletion deletes the child NetworkConfiguration and removes the finalizer.
func (r *ControlPlaneVirtualSharedIPReconciler) handleDeletion(
	ctx context.Context,
	cpvip *vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP,
	log logr.Logger,
) (ctrl.Result, error) {
	log.Info("handling deletion of ControlPlaneVirtualSharedIP", "name", cpvip.Name)

	if !viticommonfinalizers.Has(cpvip, cpvipFinalizerName) {
		return ctrl.Result{}, nil
	}

	// Delete the child NetworkConfiguration to release the VIP IP
	ncName := cpvip.Spec.ClusterIdentifier + "-cpvip"
	nc := &vitistackcrdsv1alpha1.NetworkConfiguration{}
	if err := r.Get(ctx, client.ObjectKey{Name: ncName, Namespace: cpvip.Namespace}, nc); err == nil {
		if err := r.Delete(ctx, nc); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "failed to delete VIP NetworkConfiguration", "name", ncName)
			return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
		}
		log.Info("deleted VIP NetworkConfiguration", "name", ncName)
	} else if !apierrors.IsNotFound(err) {
		log.Error(err, "failed to get VIP NetworkConfiguration during deletion", "name", ncName)
		return ctrl.Result{RequeueAfter: cpvipRequeueDelay}, nil
	}

	// Remove finalizer
	if err := viticommonfinalizers.Remove(ctx, r.Client, cpvip, cpvipFinalizerName); err != nil {
		log.Error(err, "failed to remove finalizer")
		return reconcileutil.Requeue(err)
	}

	log.Info("removed finalizer, deletion complete")
	return ctrl.Result{}, nil
}

// setCPVIPStatus patches the CPVIP status with the given fields.
func (r *ControlPlaneVirtualSharedIPReconciler) setCPVIPStatus(
	ctx context.Context,
	cpvip *vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP,
	phase, status, message string,
	loadBalancerIps []string,
	observedGeneration int64,
) {
	log := logf.FromContext(ctx)

	base := cpvip.DeepCopy()
	updated := cpvip.DeepCopy()

	updated.Status.Phase = phase
	updated.Status.Status = status
	updated.Status.Message = message
	if loadBalancerIps != nil {
		updated.Status.LoadBalancerIps = loadBalancerIps
	}
	if observedGeneration > 0 {
		updated.Status.ObservedGeneration = observedGeneration
	}
	if updated.Status.Created.IsZero() {
		updated.Status.Created = metav1.Now()
	}

	// Pass through spec fields to status for observability
	updated.Status.ClusterIdentifier = cpvip.Spec.ClusterIdentifier
	updated.Status.SupervisorIdentifier = cpvip.Spec.SupervisorIdentifier
	updated.Status.DatacenterIdentifier = cpvip.Spec.DatacenterIdentifier
	updated.Status.NetworkNamespaceIdentifier = cpvip.Spec.NetworkNamespaceIdentifier
	updated.Status.Environment = cpvip.Spec.Environment
	updated.Status.Method = cpvip.Spec.Method
	updated.Status.PoolMembers = cpvip.Spec.PoolMembers

	// Build Ready condition
	cond := buildCPVIPReadyCondition(phase, message, cpvip.GetGeneration())
	viticommonconditions.SetOrUpdateCondition(&updated.Status.Conditions, &cond)

	if err := r.Status().Patch(ctx, updated, client.MergeFrom(base)); err != nil {
		log.Error(err, "failed to patch CPVIP status",
			"name", cpvip.Name, "namespace", cpvip.Namespace, "phase", phase)
		return
	}
	cpvip.Status = updated.Status
	cpvip.SetResourceVersion(updated.GetResourceVersion())
}

func buildCPVIPReadyCondition(phase, message string, generation int64) metav1.Condition {
	cond := metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionUnknown,
		Reason:             conditionReasonReconciling,
		Message:            "Reconciling resource",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: generation,
	}
	switch strings.ToLower(phase) {
	case "ready":
		cond.Status = metav1.ConditionTrue
		cond.Reason = conditionReasonConfigured
		if message != "" {
			cond.Message = message
		}
	case "error":
		cond.Status = metav1.ConditionFalse
		cond.Reason = conditionReasonError
		if message != "" {
			cond.Message = message
		}
	case "pending":
		cond.Status = metav1.ConditionFalse
		cond.Reason = conditionReasonReconciling
		if message != "" {
			cond.Message = message
		}
	}
	return cond
}

// NewControlPlaneVirtualSharedIPReconciler creates a new reconciler.
func NewControlPlaneVirtualSharedIPReconciler(mgr ctrl.Manager) *ControlPlaneVirtualSharedIPReconciler {
	return &ControlPlaneVirtualSharedIPReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}
}

// SetupWithManager registers the controller.
func (r *ControlPlaneVirtualSharedIPReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha1.ControlPlaneVirtualSharedIP{}).
		Owns(&vitistackcrdsv1alpha1.NetworkConfiguration{}).
		Named("controlplanevirtualsharedip").
		Complete(r)
}
