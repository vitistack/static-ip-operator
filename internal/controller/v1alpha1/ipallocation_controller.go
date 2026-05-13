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
	"encoding/binary"
	"fmt"
	"net"
	"time"

	viticommonconditions "github.com/vitistack/common/pkg/operator/conditions"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	vitistackcrdsv1alpha2 "github.com/vitistack/common/pkg/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	ipaFinalizerName = "vitistack.io/ipallocation-finalizer"
	ipaRequeueDelay  = 10 * time.Second
)

// IPAllocationReconciler reconciles IPAllocation resources. It watches
// IPAllocation CRs, looks up the referenced NetworkNamespace for pool
// configuration, and allocates the next available IP from the static range.
type IPAllocationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=vitistack.io,resources=ipallocations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vitistack.io,resources=ipallocations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=ipallocations/finalizers,verbs=update
// +kubebuilder:rbac:groups=vitistack.io,resources=networknamespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=vitistack.io,resources=networknamespaces/status,verbs=get;update;patch

func (r *IPAllocationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	ipa := &vitistackcrdsv1alpha2.IPAllocation{}
	if err := r.Get(ctx, req.NamespacedName, ipa); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Already allocated and not expiring soon — nothing to do
	if ipa.Status.Phase == vitistackcrdsv1alpha2.IPAllocationPhaseAllocated && ipa.Status.Address != "" {
		if ipa.Status.ExpiresAt == nil || time.Until(ipa.Status.ExpiresAt.Time) > ipaRequeueDelay {
			return ctrl.Result{}, nil
		}
		// Near expiry — fall through to renew
	}

	// Fetch the NetworkNamespace
	nnName := ipa.Spec.NetworkNamespaceName
	nn := &vitistackcrdsv1alpha1.NetworkNamespace{}
	if err := r.Get(ctx, client.ObjectKey{Name: nnName, Namespace: req.Namespace}, nn); err != nil {
		log.Error(err, "failed to get NetworkNamespace", "name", nnName)
		r.setIPAStatus(ctx, ipa, vitistackcrdsv1alpha2.IPAllocationPhaseError,
			"", fmt.Sprintf("NetworkNamespace %q not found: %v", nnName, err))
		return ctrl.Result{RequeueAfter: ipaRequeueDelay}, nil
	}

	// Gate on provisioningPhase
	if nn.Status.ProvisioningPhase != string(vitistackcrdsv1alpha2.ProvisioningPhaseReady) {
		log.V(1).Info("NetworkNamespace not yet provisioned, waiting",
			"networkNamespace", nnName, "provisioningPhase", nn.Status.ProvisioningPhase)
		r.setIPAStatus(ctx, ipa, vitistackcrdsv1alpha2.IPAllocationPhasePending,
			"", fmt.Sprintf("waiting for NetworkNamespace %q provisioning (phase: %s)", nnName, nn.Status.ProvisioningPhase))
		return ctrl.Result{RequeueAfter: ipaRequeueDelay}, nil
	}

	// Validate static IP config exists
	if nn.Spec.IPAllocation == nil || nn.Spec.IPAllocation.Static == nil {
		log.Error(nil, "NetworkNamespace has no static IP configuration",
			"networkNamespace", nnName)
		r.setIPAStatus(ctx, ipa, vitistackcrdsv1alpha2.IPAllocationPhaseError,
			"", fmt.Sprintf("NetworkNamespace %q has no static IP configuration", nnName))
		return ctrl.Result{}, nil
	}

	staticCfg := nn.Spec.IPAllocation.Static

	// Parse IP range
	rangeStart, rangeEnd, err := parseIPRange(staticCfg)
	if err != nil {
		log.Error(err, "invalid static IP range", "networkNamespace", nnName)
		r.setIPAStatus(ctx, ipa, vitistackcrdsv1alpha2.IPAllocationPhaseError,
			"", fmt.Sprintf("invalid IP range: %v", err))
		return ctrl.Result{}, nil
	}

	// Collect all existing allocations for this NetworkNamespace
	existingIPAs := &vitistackcrdsv1alpha2.IPAllocationList{}
	if err := r.List(ctx, existingIPAs, client.InNamespace(req.Namespace),
		client.MatchingLabels{vitistackcrdsv1alpha2.LabelNetworkNamespace: nnName}); err != nil {
		log.Error(err, "failed to list existing IPAllocations")
		return ctrl.Result{RequeueAfter: ipaRequeueDelay}, nil
	}

	allocated := make(map[string]struct{})
	for i := range existingIPAs.Items {
		existing := &existingIPAs.Items[i]
		if existing.Name == ipa.Name {
			continue // Skip self
		}
		if existing.Status.Phase == vitistackcrdsv1alpha2.IPAllocationPhaseAllocated && existing.Status.Address != "" {
			allocated[existing.Status.Address] = struct{}{}
		}
	}

	// Try to keep existing allocation if still valid
	var assignedIP string
	if ipa.Status.Address != "" {
		ip := net.ParseIP(ipa.Status.Address)
		if ip != nil && isIPInRange(ip, rangeStart, rangeEnd) {
			if _, taken := allocated[ipa.Status.Address]; !taken {
				assignedIP = ipa.Status.Address
				log.V(1).Info("keeping existing IP allocation", "ip", assignedIP)
			}
		}
	}

	// Honor requested address if set
	if assignedIP == "" && ipa.Spec.RequestedAddress != "" {
		reqIP := net.ParseIP(ipa.Spec.RequestedAddress)
		if reqIP != nil && isIPInRange(reqIP, rangeStart, rangeEnd) {
			if _, taken := allocated[ipa.Spec.RequestedAddress]; !taken {
				assignedIP = ipa.Spec.RequestedAddress
				log.Info("honoring requested address", "ip", assignedIP)
			} else {
				log.Info("requested address is already taken", "ip", ipa.Spec.RequestedAddress)
			}
		}
	}

	// Allocate next available
	if assignedIP == "" {
		assignedIP = findNextAvailableIP(rangeStart, rangeEnd, allocated, staticCfg.IPv4Gateway)
		if assignedIP == "" {
			log.Error(nil, "no available IPs in range",
				"networkNamespace", nnName, "allocated", len(allocated))
			r.setIPAStatus(ctx, ipa, vitistackcrdsv1alpha2.IPAllocationPhaseError,
				"", "no available IP addresses in pool")
			return ctrl.Result{RequeueAfter: ipaRequeueDelay}, nil
		}
		log.Info("allocated new IP", "ip", assignedIP, "networkNamespace", nnName)
	}

	// Compute prefix length from CIDR
	_, ipNet, _ := net.ParseCIDR(staticCfg.IPv4CIDR)
	prefixLen, _ := ipNet.Mask.Size()

	// Set expiry
	var expiresAt *metav1.Time
	if staticCfg.TTLSeconds > 0 {
		t := metav1.NewTime(time.Now().Add(time.Duration(staticCfg.TTLSeconds) * time.Second))
		expiresAt = &t
	}

	// Write status
	base := ipa.DeepCopy()
	ipa.Status.Phase = vitistackcrdsv1alpha2.IPAllocationPhaseAllocated
	ipa.Status.Address = assignedIP
	ipa.Status.Gateway = staticCfg.IPv4Gateway
	ipa.Status.Prefix = prefixLen
	ipa.Status.VlanID = staticCfg.VlanID
	ipa.Status.DNS = staticCfg.DNS
	ipa.Status.ExpiresAt = expiresAt
	ipa.Status.Message = "IP allocated successfully"

	if err := r.Status().Patch(ctx, ipa, client.MergeFrom(base)); err != nil {
		log.Error(err, "failed to update IPAllocation status")
		return ctrl.Result{RequeueAfter: ipaRequeueDelay}, nil
	}

	// Update the NetworkNamespace summary
	if err := r.updateNNSummary(ctx, nn, staticCfg, rangeStart, rangeEnd, existingIPAs, ipa); err != nil {
		log.Error(err, "failed to update NetworkNamespace IP allocation summary")
	}

	// Requeue before TTL expires to renew
	if staticCfg.TTLSeconds > 0 {
		ttl := time.Duration(staticCfg.TTLSeconds) * time.Second
		if half := ttl / 2; half > 0 {
			return ctrl.Result{RequeueAfter: half}, nil
		}
	}

	return ctrl.Result{}, nil
}

// updateNNSummary updates the NetworkNamespace's ipAllocationSummary (counts only).
func (r *IPAllocationReconciler) updateNNSummary(
	ctx context.Context,
	nn *vitistackcrdsv1alpha1.NetworkNamespace,
	staticCfg *vitistackcrdsv1alpha1.StaticIPAllocationConfig,
	rangeStart, rangeEnd net.IP,
	existingIPAs *vitistackcrdsv1alpha2.IPAllocationList,
	currentIPA *vitistackcrdsv1alpha2.IPAllocation,
) error {
	totalCount := int32(ipCountFromIPs(rangeStart, rangeEnd))
	allocatedCount := int32(0)
	for i := range existingIPAs.Items {
		if existingIPAs.Items[i].Status.Phase == vitistackcrdsv1alpha2.IPAllocationPhaseAllocated {
			allocatedCount++
		}
	}
	// Include current if it wasn't in the list (new allocation)
	if currentIPA.Status.Phase == vitistackcrdsv1alpha2.IPAllocationPhaseAllocated {
		found := false
		for i := range existingIPAs.Items {
			if existingIPAs.Items[i].Name == currentIPA.Name {
				found = true
				break
			}
		}
		if !found {
			allocatedCount++
		}
	}

	base := nn.DeepCopy()
	updated := nn.DeepCopy()

	// Write ipv4Prefix and vlanId from static config
	updated.Status.IPv4Prefix = staticCfg.IPv4CIDR
	updated.Status.VlanID = staticCfg.VlanID

	if updated.Status.IPAllocationStatus == nil {
		updated.Status.IPAllocationStatus = &vitistackcrdsv1alpha1.NetworkNamespaceIPAllocationStatus{}
	}
	updated.Status.IPAllocationStatus.Type = vitistackcrdsv1alpha1.IPAllocationTypeStatic
	updated.Status.IPAllocationStatus.Provider = vitistackcrdsv1alpha1.ProviderNameStaticIP
	updated.Status.IPAllocationStatus.TotalCount = totalCount
	updated.Status.IPAllocationStatus.AllocatedCount = allocatedCount
	updated.Status.IPAllocationStatus.AvailableCount = totalCount - allocatedCount
	// No more AllocatedIPs array — that data lives in IPAllocation CRs now

	return r.Status().Patch(ctx, updated, client.MergeFrom(base))
}

func (r *IPAllocationReconciler) setIPAStatus(ctx context.Context, ipa *vitistackcrdsv1alpha2.IPAllocation, phase vitistackcrdsv1alpha2.IPAllocationPhase, address, message string) {
	base := ipa.DeepCopy()
	ipa.Status.Phase = phase
	if address != "" {
		ipa.Status.Address = address
	}
	ipa.Status.Message = message
	_ = r.Status().Patch(ctx, ipa, client.MergeFrom(base))
}

// ipCountFromIPs counts IPs between two IP addresses (inclusive).
func ipCountFromIPs(rangeStart, rangeEnd net.IP) int {
	start := binary.BigEndian.Uint32(rangeStart.To4())
	end := binary.BigEndian.Uint32(rangeEnd.To4())
	return int(end-start) + 1
}

// NewIPAllocationReconciler creates a new reconciler.
func NewIPAllocationReconciler(mgr ctrl.Manager) *IPAllocationReconciler {
	return &IPAllocationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}
}

// SetupWithManager registers the controller.
func (r *IPAllocationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Ensure v1alpha2 types are registered
	if err := vitistackcrdsv1alpha2.AddToScheme(mgr.GetScheme()); err != nil {
		return fmt.Errorf("registering v1alpha2 scheme: %w", err)
	}

	// Set Ready condition on the controller for health checks
	_ = viticommonconditions.New("Ready", metav1.ConditionTrue, "Configured", "controller started", 0)

	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha2.IPAllocation{}).
		Named("ipallocation").
		Complete(r)
}
