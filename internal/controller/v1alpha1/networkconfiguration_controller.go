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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	viticommonconditions "github.com/vitistack/common/pkg/operator/conditions"
	viticommonfinalizers "github.com/vitistack/common/pkg/operator/finalizers"
	reconcileutil "github.com/vitistack/common/pkg/operator/reconcileutil"
	vitistackcrdsv1alpha1 "github.com/vitistack/common/pkg/v1alpha1"
	"github.com/vitistack/static-ip-operator/internal/consts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// deprecationWarned tracks resources for which a deprecation notice has already
// been logged, so we don't spam the logs on every reconcile loop.
var deprecationWarned sync.Map

// strictDefaultsEnabled reports whether STATIC_IP_STRICT_DEFAULTS is set to a
// truthy value. When true, the operator refuses to treat unset fields as
// defaults and instead surfaces an error — forcing migration to explicit config.
func strictDefaultsEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(consts.STATIC_IP_STRICT_DEFAULTS)))
	return v == "true" || v == "1" || v == "yes"
}

const (
	finalizerName              = "vitistack.io/static-ip-finalizer"
	conditionTypeReady         = "Ready"
	conditionReasonReconciling = "Reconciling"
	conditionReasonConfigured  = "Configured"
	conditionReasonError       = "Error"

	RequeueDelay = 10 * time.Second
)

// NetworkConfigurationReconciler reconciles NetworkConfiguration resources
// for static IP allocation. It watches NetworkConfigurations whose
// NetworkNamespace has ipAllocation.provider set to "static", and allocates
// the next available IP from the configured range.
type NetworkConfigurationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=vitistack.io,resources=networkconfigurations,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=networkconfigurations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vitistack.io,resources=networkconfigurations/finalizers,verbs=update
// +kubebuilder:rbac:groups=vitistack.io,resources=networknamespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=vitistack.io,resources=networknamespaces/status,verbs=get;update;patch

func (r *NetworkConfigurationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	nc := &vitistackcrdsv1alpha1.NetworkConfiguration{}
	if err := r.Get(ctx, req.NamespacedName, nc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Provider triage — if spec.provider is explicitly set to something other
	// than 'static-ip-operator', this NC belongs to another operator. Skip
	// silently so we don't spam logs for resources that aren't ours.
	if vitistackcrdsv1alpha1.IsProviderSet(nc.Spec.Provider) &&
		!vitistackcrdsv1alpha1.MatchesProvider(nc.Spec.Provider, vitistackcrdsv1alpha1.ProviderNameStaticIP) {
		log.V(1).Info("skipping NetworkConfiguration, provider is not 'static-ip-operator'",
			"provider", nc.Spec.Provider, "name", nc.Name, "namespace", req.Namespace)
		return ctrl.Result{}, nil
	}

	// Handle deletion before triage: if the NC has our finalizer we must run
	// cleanup regardless of the NN's current state.
	if !nc.GetDeletionTimestamp().IsZero() {
		return r.handleDeletion(ctx, nc, log)
	}

	// Fetch the NetworkNamespace silently. We need it to decide whether this
	// NC is actually ours (type=static) before logging anything.
	nn, fallbackUsed, fallbackCount, err := r.getNetworkNamespace(ctx, req.Namespace, nc.Spec.NetworkNamespaceName)
	if err != nil {
		// Can't determine ownership — log at V(1) so we don't spam for
		// resources that likely aren't ours.
		log.V(1).Info("unable to fetch NetworkNamespace for triage",
			"networkConfiguration", nc.Name, "networkNamespaceName", nc.Spec.NetworkNamespaceName, "namespace", req.Namespace, "error", err.Error())
		return ctrl.Result{}, nil
	}

	// Type triage — this operator only handles type=static. Anything else
	// (nil → DHCP default, explicit dhcp, anything non-static) belongs to
	// another operator. Skip silently.
	if nn.Spec.IPAllocation == nil {
		log.V(1).Info("skipping NetworkConfiguration, NetworkNamespace has no ipAllocation (DHCP default)",
			"networkNamespace", nn.Name, "networkConfiguration", nc.Name, "namespace", req.Namespace)
		return ctrl.Result{}, nil
	}
	if nn.Spec.IPAllocation.Type != vitistackcrdsv1alpha1.IPAllocationTypeStatic {
		log.V(1).Info("skipping NetworkConfiguration, NetworkNamespace ipAllocation type is not 'static'",
			"type", nn.Spec.IPAllocation.Type, "networkNamespace", nn.Name, "networkConfiguration", nc.Name, "namespace", req.Namespace)
		return ctrl.Result{}, nil
	}

	// --- Past triage: this NC is ours (type=static). ---

	log.Info("reconciling NetworkConfiguration",
		"name", nc.Name, "namespace", req.Namespace, "generation", nc.GetGeneration())

	log.Info("found NetworkNamespace",
		"networkNamespace", nn.Name, "networkConfiguration", nc.Name, "namespace", req.Namespace)

	// Warn (once) about the fallback NN lookup when we're actually handling
	// this NC, so we don't emit it for resources that belong elsewhere.
	if fallbackUsed {
		if _, alreadyWarned := deprecationWarned.LoadOrStore("nnname-"+req.Namespace, true); !alreadyWarned {
			log.Info("WARNING: NetworkConfiguration has no spec.networkNamespaceName set; "+
				"falling back to listing all NetworkNamespaces in namespace for backward compatibility. "+
				"Please set spec.networkNamespaceName explicitly. "+
				"Set STATIC_IP_STRICT_DEFAULTS=true to force migration.",
				"namespace", req.Namespace)
		}
		if fallbackCount > 1 {
			log.Info("WARNING: multiple NetworkNamespaces found in namespace, using first one",
				"namespace", req.Namespace, "count", fallbackCount, "selected", nn.Name)
		}
	}

	staticCfg := nn.Spec.IPAllocation.Static
	if staticCfg == nil {
		log.Error(nil, "NetworkNamespace has ipAllocation.type 'static' but missing static configuration block",
			"networkNamespace", nn.Name, "networkConfiguration", nc.Name, "namespace", req.Namespace,
			"required", "ipv4CIDR, ipv4Gateway", "optional", "ipv4RangeStart, ipv4RangeEnd")
		r.updateStatusWithLog(ctx, log, nc, "Error", "Failed",
			fmt.Sprintf("NetworkNamespace %s has type 'static' but no static configuration (ipv4CIDR, ipv4Gateway required)", nn.Name), nil)
		return ctrl.Result{RequeueAfter: RequeueDelay}, nil
	}

	// Ensure finalizer
	if !viticommonfinalizers.Has(nc, finalizerName) {
		if err := viticommonfinalizers.Ensure(ctx, r.Client, nc, finalizerName); err != nil {
			log.Error(err, "failed to ensure finalizer",
				"networkConfiguration", nc.Name, "namespace", req.Namespace)
			return reconcileutil.Requeue(err)
		}
		log.Info("added finalizer to NetworkConfiguration",
			"networkConfiguration", nc.Name, "namespace", req.Namespace)
		return ctrl.Result{}, nil
	}

	// Set reconciling condition
	if ready := findCondition(nc.Status.Conditions, conditionTypeReady); ready == nil || ready.ObservedGeneration != nc.GetGeneration() {
		r.setConditionWithLog(ctx, log, nc, viticommonconditions.New(
			conditionTypeReady, metav1.ConditionFalse, conditionReasonReconciling, "reconciling", nc.GetGeneration(),
		))
	}

	// Collect all IPs already allocated by other NetworkConfigurations in this namespace
	allocatedIPs, err := r.collectAllocatedIPs(ctx, req.Namespace, nc.Spec.NetworkNamespaceName, nc.Name)
	if err != nil {
		log.Error(err, "failed to collect allocated IPs",
			"networkConfiguration", nc.Name, "networkNamespace", nn.Name, "namespace", req.Namespace)
		r.updateStatusWithLog(ctx, log, nc, "Error", "Failed", fmt.Sprintf("failed to collect allocated IPs: %v", err), nil)
		return ctrl.Result{RequeueAfter: RequeueDelay}, nil
	}

	log.Info("collected existing IP allocations",
		"networkConfiguration", nc.Name, "networkNamespace", nn.Name, "allocatedCount", len(allocatedIPs))

	// Parse the IP range
	rangeStart, rangeEnd, err := parseIPRange(staticCfg)
	if err != nil {
		log.Error(err, "invalid static IP configuration",
			"networkNamespace", nn.Name, "networkConfiguration", nc.Name, "namespace", req.Namespace,
			"cidr", staticCfg.IPv4CIDR, "rangeStart", staticCfg.IPv4RangeStart, "rangeEnd", staticCfg.IPv4RangeEnd)
		r.updateStatusWithLog(ctx, log, nc, "Error", "Failed", fmt.Sprintf("invalid static IP config: %v", err), nil)
		return ctrl.Result{RequeueAfter: RequeueDelay}, nil
	}

	log.Info("parsed IP range",
		"networkNamespace", nn.Name, "cidr", staticCfg.IPv4CIDR,
		"rangeStart", rangeStart.String(), "rangeEnd", rangeEnd.String(),
		"totalIPs", ipCount(rangeStart, rangeEnd))

	// Allocate IPs for each interface that needs one
	statusInterfaces, allocCount, errMsgs := r.allocateInterfaces(nc, staticCfg, allocatedIPs, rangeStart, rangeEnd)

	if len(errMsgs) > 0 {
		errMsg := strings.Join(errMsgs, "; ")
		log.Error(nil, "IP allocation failed for one or more interfaces",
			"networkConfiguration", nc.Name, "networkNamespace", nn.Name, "namespace", req.Namespace,
			"errors", errMsg)
		r.setConditionWithLog(ctx, log, nc, viticommonconditions.New(
			conditionTypeReady, metav1.ConditionFalse, conditionReasonError, errMsg, nc.GetGeneration(),
		))
		r.updateStatusWithLog(ctx, log, nc, "Error", "Failed", errMsg, statusInterfaces)
		return ctrl.Result{RequeueAfter: RequeueDelay}, nil
	}

	// Collect the full list of allocated IP entries (including the ones just allocated)
	allocatedIPEntries, err := r.collectAllocatedIPEntries(ctx, req.Namespace, nc.Spec.NetworkNamespaceName, "")
	if err != nil {
		log.Error(err, "failed to collect allocated IP entries",
			"networkConfiguration", nc.Name, "networkNamespace", nn.Name, "namespace", req.Namespace)
	}

	// Update NetworkNamespace IP allocation status + network fields (ipv4Prefix, vlanId)
	if err := r.updateNetworkNamespaceIPAllocationStatus(ctx, nn, staticCfg, allocatedIPs, allocCount, rangeStart, rangeEnd, allocatedIPEntries); err != nil {
		log.Error(err, "failed to update NetworkNamespace IP allocation status",
			"networkNamespace", nn.Name, "networkConfiguration", nc.Name, "namespace", req.Namespace)
	}

	totalIPs := ipCount(rangeStart, rangeEnd)
	totalAllocated := len(allocatedIPs) + allocCount
	available := totalIPs - totalAllocated

	msg := fmt.Sprintf("All %d interfaces allocated static IPs", allocCount)

	log.Info("successfully allocated static IPs",
		"networkConfiguration", nc.Name, "networkNamespace", nn.Name, "namespace", req.Namespace,
		"interfacesAllocated", allocCount, "totalAllocated", totalAllocated,
		"totalIPs", totalIPs, "availableIPs", available)

	logIPPoolUtilization(log, nn.Name, req.Namespace, totalAllocated, totalIPs, available)

	r.setConditionWithLog(ctx, log, nc, viticommonconditions.New(
		conditionTypeReady, metav1.ConditionTrue, conditionReasonConfigured, "configured", nc.GetGeneration(),
	))
	r.updateStatusWithLog(ctx, log, nc, "Ready", "Success", msg, statusInterfaces)

	// Requeue before TTL expires to renew allocations
	requeueAfter := RequeueDelay
	if staticCfg.TTLSeconds > 0 {
		ttl := time.Duration(staticCfg.TTLSeconds) * time.Second
		// Requeue at half the TTL to renew before expiry
		if half := ttl / 2; half > 0 && half < requeueAfter {
			requeueAfter = half
		}
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// allocateInterfaces assigns a static IP to each interface on the NetworkConfiguration.
// It tries to keep existing allocations stable (if the interface already has a valid
// static IP within range, it keeps it).
func (r *NetworkConfigurationReconciler) allocateInterfaces(
	nc *vitistackcrdsv1alpha1.NetworkConfiguration,
	staticCfg *vitistackcrdsv1alpha1.StaticIPAllocationConfig,
	allocatedIPs map[string]struct{},
	rangeStart, rangeEnd net.IP,
) ([]vitistackcrdsv1alpha1.NetworkConfigurationInterface, int, []string) {
	log := logf.Log.WithName("allocateInterfaces").WithValues(
		"networkConfiguration", nc.Name, "namespace", nc.Namespace)

	_, ipNet, _ := net.ParseCIDR(staticCfg.IPv4CIDR)

	var statusInterfaces []vitistackcrdsv1alpha1.NetworkConfigurationInterface
	var errMsgs []string
	allocCount := 0

	// Build a set of IPs already assigned to this NC's interfaces (from status)
	// so we can try to keep them stable
	existingIPs := make(map[string]string) // iface name -> IP
	for _, si := range nc.Status.NetworkInterfaces {
		if si.AllocationMethod == vitistackcrdsv1alpha1.IPAllocationTypeStatic && len(si.IPv4Addresses) > 0 {
			existingIPs[si.Name] = si.IPv4Addresses[0]
		}
	}

	var expiry *metav1.Time
	if staticCfg.TTLSeconds > 0 {
		t := metav1.NewTime(time.Now().Add(time.Duration(staticCfg.TTLSeconds) * time.Second))
		expiry = &t
	}

	for _, iface := range nc.Spec.NetworkInterfaces {
		statusIface := vitistackcrdsv1alpha1.NetworkConfigurationInterface{
			Name:       iface.Name,
			MacAddress: iface.MacAddress,
			Vlan:       iface.Vlan,
		}

		// Try to keep the existing allocation if still in range and not taken by others
		var assignedIP string
		if existing, ok := existingIPs[iface.Name]; ok {
			ip := net.ParseIP(existing)
			if ip != nil && isIPInRange(ip, rangeStart, rangeEnd) {
				if _, taken := allocatedIPs[existing]; !taken {
					assignedIP = existing
					log.Info("keeping existing IP allocation for interface",
						"interface", iface.Name, "ip", assignedIP)
				} else {
					log.Info("WARNING: existing IP for interface is now taken by another NetworkConfiguration, will reallocate",
						"interface", iface.Name, "previousIP", existing)
				}
			} else if ip != nil {
				log.Info("WARNING: existing IP for interface is outside current range, will reallocate",
					"interface", iface.Name, "previousIP", existing,
					"rangeStart", rangeStart.String(), "rangeEnd", rangeEnd.String())
			}
		}

		// Otherwise allocate the next available
		if assignedIP == "" {
			nextIP := findNextAvailableIP(rangeStart, rangeEnd, allocatedIPs, staticCfg.IPv4Gateway)
			if nextIP == "" {
				log.Error(nil, "no available IPs in range for interface",
					"interface", iface.Name,
					"rangeStart", rangeStart.String(), "rangeEnd", rangeEnd.String(),
					"allocatedCount", len(allocatedIPs))
				errMsgs = append(errMsgs, fmt.Sprintf("no available IPs for interface %s", iface.Name))
				statusInterfaces = append(statusInterfaces, statusIface)
				continue
			}
			assignedIP = nextIP
			log.Info("allocated new IP for interface",
				"interface", iface.Name, "ip", assignedIP)
		}

		// Mark as allocated so subsequent interfaces don't get the same IP
		allocatedIPs[assignedIP] = struct{}{}

		statusIface.IPv4Addresses = []string{assignedIP}
		statusIface.IPv4Subnet = ipNet.String()
		statusIface.IPv4Gateway = staticCfg.IPv4Gateway
		statusIface.DNS = staticCfg.DNS
		statusIface.IPAllocated = true
		statusIface.AllocationMethod = vitistackcrdsv1alpha1.IPAllocationTypeStatic
		statusIface.AllocationExpiry = expiry
		allocCount++

		statusInterfaces = append(statusInterfaces, statusIface)
	}

	return statusInterfaces, allocCount, errMsgs
}

// collectAllocatedIPs lists all NetworkConfigurations in the namespace that reference
// the same NetworkNamespace, and collects their allocated static IPs.
// The excludeName parameter is used to exclude the current NC from the set.
func (r *NetworkConfigurationReconciler) collectAllocatedIPs(ctx context.Context, namespace, networkNamespaceName, excludeName string) (map[string]struct{}, error) {
	ncList := &vitistackcrdsv1alpha1.NetworkConfigurationList{}
	if err := r.List(ctx, ncList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	allocated := make(map[string]struct{})
	for _, nc := range ncList.Items {
		if nc.Name == excludeName {
			continue
		}
		// Only consider NCs referencing the same NetworkNamespace
		if nc.Spec.NetworkNamespaceName != networkNamespaceName {
			continue
		}
		for _, iface := range nc.Status.NetworkInterfaces {
			if iface.AllocationMethod == vitistackcrdsv1alpha1.IPAllocationTypeStatic {
				for _, ip := range iface.IPv4Addresses {
					allocated[ip] = struct{}{}
				}
			}
		}
	}

	return allocated, nil
}

// collectAllocatedIPEntries lists all allocated IPs with their owning NetworkConfiguration names.
func (r *NetworkConfigurationReconciler) collectAllocatedIPEntries(ctx context.Context, namespace, networkNamespaceName, excludeName string) ([]vitistackcrdsv1alpha1.AllocatedIPEntry, error) {
	ncList := &vitistackcrdsv1alpha1.NetworkConfigurationList{}
	if err := r.List(ctx, ncList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	var entries []vitistackcrdsv1alpha1.AllocatedIPEntry
	for _, nc := range ncList.Items {
		if nc.Name == excludeName {
			continue
		}
		if nc.Spec.NetworkNamespaceName != networkNamespaceName {
			continue
		}
		for _, iface := range nc.Status.NetworkInterfaces {
			if iface.AllocationMethod == vitistackcrdsv1alpha1.IPAllocationTypeStatic {
				for _, ip := range iface.IPv4Addresses {
					entries = append(entries, vitistackcrdsv1alpha1.AllocatedIPEntry{
						IP:                   ip,
						NetworkConfiguration: nc.Name,
					})
				}
			}
		}
	}

	return entries, nil
}

// handleDeletion removes the finalizer and recalculates the NetworkNamespace IP allocation status.
func (r *NetworkConfigurationReconciler) handleDeletion(ctx context.Context, nc *vitistackcrdsv1alpha1.NetworkConfiguration, log logr.Logger) (ctrl.Result, error) {
	log.Info("handling deletion of NetworkConfiguration", "name", nc.Name, "namespace", nc.Namespace)
	if err := viticommonfinalizers.Remove(ctx, r.Client, nc, finalizerName); err != nil {
		log.Error(err, "failed to remove finalizer during deletion",
			"name", nc.Name, "namespace", nc.Namespace)
		return reconcileutil.Requeue(err)
	}
	log.Info("removed finalizer, deletion complete", "name", nc.Name, "namespace", nc.Namespace)

	// Recalculate NetworkNamespace IP allocation status now that this NC's IPs are released
	nn, _, _, err := r.getNetworkNamespace(ctx, nc.Namespace, nc.Spec.NetworkNamespaceName)
	if err != nil {
		log.Error(err, "failed to get NetworkNamespace during deletion cleanup, status may be stale",
			"name", nc.Name, "namespace", nc.Namespace)
		return ctrl.Result{}, nil
	}
	if nn.Spec.IPAllocation == nil || nn.Spec.IPAllocation.Static == nil {
		return ctrl.Result{}, nil
	}
	staticCfg := nn.Spec.IPAllocation.Static

	rangeStart, rangeEnd, err := parseIPRange(staticCfg)
	if err != nil {
		log.Error(err, "failed to parse IP range during deletion cleanup")
		return ctrl.Result{}, nil
	}

	// Collect remaining allocations (the deleted NC is excluded because its finalizer is gone)
	allocatedIPs, err := r.collectAllocatedIPs(ctx, nc.Namespace, nc.Spec.NetworkNamespaceName, nc.Name)
	if err != nil {
		log.Error(err, "failed to collect allocated IPs during deletion cleanup")
		return ctrl.Result{}, nil
	}

	// Also collect the ownership map for the status
	allocatedIPEntries, err := r.collectAllocatedIPEntries(ctx, nc.Namespace, nc.Spec.NetworkNamespaceName, nc.Name)
	if err != nil {
		log.Error(err, "failed to collect allocated IP entries during deletion cleanup")
		return ctrl.Result{}, nil
	}

	if err := r.updateNetworkNamespaceIPAllocationStatus(ctx, nn, staticCfg, allocatedIPs, 0, rangeStart, rangeEnd, allocatedIPEntries); err != nil {
		log.Error(err, "failed to update NetworkNamespace IP allocation status during deletion cleanup")
	}

	return ctrl.Result{}, nil
}

// getNetworkNamespace fetches the NetworkNamespace by name or via legacy list
// fallback. Returns:
//   - nn: the fetched NetworkNamespace.
//   - fallbackUsed: true if the legacy list-and-pick-first path was taken.
//   - listCount: when fallbackUsed is true, the number of NNs found in the
//     namespace; otherwise 0. Callers can use this to decide whether to warn
//     about ambiguity (multiple NNs in one namespace).
//   - err: any fetch/list error, or a strict-mode refusal when
//     STATIC_IP_STRICT_DEFAULTS is enabled and networkNamespaceName is empty.
//
// No logs are emitted here so callers can silently triage NCs that belong to
// another operator and only warn once ownership is confirmed.
func (r *NetworkConfigurationReconciler) getNetworkNamespace(ctx context.Context, namespace, networkNamespaceName string) (*vitistackcrdsv1alpha1.NetworkNamespace, bool, int, error) {
	var nn vitistackcrdsv1alpha1.NetworkNamespace

	if networkNamespaceName != "" {
		if err := r.Get(ctx, client.ObjectKey{Name: networkNamespaceName, Namespace: namespace}, &nn); err != nil {
			return nil, false, 0, fmt.Errorf("failed to get NetworkNamespace %s in namespace %s: %w", networkNamespaceName, namespace, err)
		}
		return &nn, false, 0, nil
	}

	if strictDefaultsEnabled() {
		return nil, true, 0, fmt.Errorf("spec.networkNamespaceName is not set and STATIC_IP_STRICT_DEFAULTS is enabled; refusing to fall back to listing NetworkNamespaces. Set spec.networkNamespaceName explicitly")
	}

	nnList := &vitistackcrdsv1alpha1.NetworkNamespaceList{}
	if err := r.List(ctx, nnList, client.InNamespace(namespace)); err != nil {
		return nil, true, 0, fmt.Errorf("failed to list NetworkNamespaces in namespace %s: %w", namespace, err)
	}
	if len(nnList.Items) == 0 {
		return nil, true, 0, fmt.Errorf("no NetworkNamespace found in namespace %s", namespace)
	}
	return &nnList.Items[0], true, len(nnList.Items), nil
}

// updateNetworkNamespaceIPAllocationStatus updates the IP allocation status counters
// and network fields (ipv4Prefix, vlanId) on the NetworkNamespace so that other
// operators (e.g. kubevirt-operator) can use them.
func (r *NetworkConfigurationReconciler) updateNetworkNamespaceIPAllocationStatus(
	ctx context.Context,
	nn *vitistackcrdsv1alpha1.NetworkNamespace,
	staticCfg *vitistackcrdsv1alpha1.StaticIPAllocationConfig,
	allocatedIPs map[string]struct{},
	newAllocations int,
	rangeStart, rangeEnd net.IP,
	allocatedIPEntries []vitistackcrdsv1alpha1.AllocatedIPEntry,
) error {
	log := logf.FromContext(ctx)

	totalCount := int32(ipCount(rangeStart, rangeEnd))                 // #nosec G115 -- max 253 for /24, well within int32
	allocatedCount := int32(len(allocatedIPs)) + int32(newAllocations) // #nosec G115 -- bounded by totalCount

	base := nn.DeepCopy()
	updated := nn.DeepCopy()

	// Populate standard NetworkNamespace status fields so kubevirt-operator
	// and other consumers can use them (ipv4Prefix for subnet info, vlanId
	// for NetworkAttachmentDefinition creation).
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
	updated.Status.IPAllocationStatus.AllocatedIPs = allocatedIPEntries

	if err := r.Status().Patch(ctx, updated, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("failed to patch NetworkNamespace %s/%s status: %w", nn.Namespace, nn.Name, err)
	}

	log.Info("updated NetworkNamespace IP allocation status",
		"networkNamespace", nn.Name, "namespace", nn.Namespace,
		"ipv4Prefix", staticCfg.IPv4CIDR, "vlanId", staticCfg.VlanID,
		"totalIPs", totalCount, "allocated", allocatedCount, "available", totalCount-allocatedCount)

	return nil
}

// NewNetworkConfigurationReconciler creates a new reconciler.
func NewNetworkConfigurationReconciler(mgr ctrl.Manager) *NetworkConfigurationReconciler {
	return &NetworkConfigurationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}
}

// SetupWithManager registers the controller.
func (r *NetworkConfigurationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vitistackcrdsv1alpha1.NetworkConfiguration{}).
		Named("networkconfiguration").
		Complete(r)
}

// --- Status helpers ---

func (r *NetworkConfigurationReconciler) setCondition(ctx context.Context, nc *vitistackcrdsv1alpha1.NetworkConfiguration, cond metav1.Condition) error {
	base := nc.DeepCopy()
	updated := nc.DeepCopy()
	viticommonconditions.SetOrUpdateCondition(&updated.Status.Conditions, &cond)

	prev := findCondition(base.Status.Conditions, cond.Type)
	cur := findCondition(updated.Status.Conditions, cond.Type)
	if prev != nil && cur != nil &&
		prev.Status == cur.Status && prev.Reason == cur.Reason &&
		prev.Message == cur.Message && prev.ObservedGeneration == cur.ObservedGeneration {
		return nil
	}

	if err := r.Status().Patch(ctx, updated, client.MergeFrom(base)); err != nil {
		return err
	}
	nc.Status.Conditions = updated.Status.Conditions
	nc.SetResourceVersion(updated.GetResourceVersion())
	return nil
}

func (r *NetworkConfigurationReconciler) updateStatus(
	ctx context.Context,
	nc *vitistackcrdsv1alpha1.NetworkConfiguration,
	phase, status, message string,
	networkInterfaces []vitistackcrdsv1alpha1.NetworkConfigurationInterface,
) error {
	base := nc.DeepCopy()
	updated := nc.DeepCopy()

	updated.Status.Phase = phase
	updated.Status.Status = status
	updated.Status.Message = message
	if updated.Status.Created.IsZero() {
		updated.Status.Created = metav1.Now()
	}
	if networkInterfaces != nil {
		updated.Status.NetworkInterfaces = networkInterfaces
	}

	if err := r.Status().Patch(ctx, updated, client.MergeFrom(base)); err != nil {
		return err
	}
	nc.Status = updated.Status
	nc.SetResourceVersion(updated.GetResourceVersion())
	return nil
}

func (r *NetworkConfigurationReconciler) setConditionWithLog(ctx context.Context, log logr.Logger, nc *vitistackcrdsv1alpha1.NetworkConfiguration, cond metav1.Condition) {
	if err := r.setCondition(ctx, nc, cond); err != nil {
		log.Error(err, "failed to set condition",
			"conditionType", cond.Type, "conditionReason", cond.Reason,
			"networkConfiguration", nc.Name, "namespace", nc.Namespace)
	}
}

func (r *NetworkConfigurationReconciler) updateStatusWithLog(
	ctx context.Context, log logr.Logger, nc *vitistackcrdsv1alpha1.NetworkConfiguration,
	phase, status, message string, networkInterfaces []vitistackcrdsv1alpha1.NetworkConfigurationInterface,
) {
	if err := r.updateStatus(ctx, nc, phase, status, message, networkInterfaces); err != nil {
		log.Error(err, "failed to update NetworkConfiguration status",
			"networkConfiguration", nc.Name, "namespace", nc.Namespace, "phase", phase)
	}
}

func logIPPoolUtilization(log logr.Logger, networkNamespace, namespace string, totalAllocated, totalIPs, available int) {
	if totalIPs <= 0 {
		return
	}
	utilization := float64(totalAllocated) / float64(totalIPs) * 100
	switch {
	case utilization >= 90:
		log.Info("WARNING: IP pool utilization is critically high",
			"networkNamespace", networkNamespace, "namespace", namespace,
			"utilization", fmt.Sprintf("%.1f%%", utilization),
			"allocated", totalAllocated, "total", totalIPs, "available", available)
	case utilization >= 75:
		log.Info("WARNING: IP pool utilization is high",
			"networkNamespace", networkNamespace, "namespace", namespace,
			"utilization", fmt.Sprintf("%.1f%%", utilization),
			"allocated", totalAllocated, "total", totalIPs, "available", available)
	}
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

// --- IP allocation helpers ---

// parseIPRange returns the start and end IPs from the static config.
func parseIPRange(cfg *vitistackcrdsv1alpha1.StaticIPAllocationConfig) (net.IP, net.IP, error) {
	_, ipNet, err := net.ParseCIDR(cfg.IPv4CIDR)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid IPv4CIDR %q: %w", cfg.IPv4CIDR, err)
	}

	var rangeStart, rangeEnd net.IP

	if cfg.IPv4RangeStart != "" {
		rangeStart = net.ParseIP(cfg.IPv4RangeStart).To4()
		if rangeStart == nil {
			return nil, nil, fmt.Errorf("invalid IPv4RangeStart %q", cfg.IPv4RangeStart)
		}
	} else {
		// Default: network address + 2 (skip network and gateway)
		rangeStart = nextIP(ipNet.IP.To4(), 2)
	}

	if cfg.IPv4RangeEnd != "" {
		rangeEnd = net.ParseIP(cfg.IPv4RangeEnd).To4()
		if rangeEnd == nil {
			return nil, nil, fmt.Errorf("invalid IPv4RangeEnd %q", cfg.IPv4RangeEnd)
		}
	} else {
		// Default: broadcast - 1
		rangeEnd = prevIP(broadcastAddr(ipNet))
	}

	if ipToUint32(rangeStart) > ipToUint32(rangeEnd) {
		return nil, nil, fmt.Errorf("rangeStart %s is after rangeEnd %s", rangeStart, rangeEnd)
	}

	return rangeStart, rangeEnd, nil
}

// findNextAvailableIP returns the next IP in the range that is not in the allocated set
// and is not the gateway. Returns "" if no IPs are available.
func findNextAvailableIP(rangeStart, rangeEnd net.IP, allocated map[string]struct{}, gateway string) string {
	start := ipToUint32(rangeStart)
	end := ipToUint32(rangeEnd)

	for i := start; i <= end; i++ {
		candidate := uint32ToIP(i).String()
		if candidate == gateway {
			continue
		}
		if _, taken := allocated[candidate]; !taken {
			return candidate
		}
	}
	return ""
}

func isIPInRange(ip, rangeStart, rangeEnd net.IP) bool {
	ipVal := ipToUint32(ip.To4())
	return ipVal >= ipToUint32(rangeStart) && ipVal <= ipToUint32(rangeEnd)
}

func ipToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	return binary.BigEndian.Uint32(ip)
}

func uint32ToIP(n uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip
}

func nextIP(ip net.IP, offset uint32) net.IP {
	return uint32ToIP(ipToUint32(ip) + offset)
}

func prevIP(ip net.IP) net.IP {
	return uint32ToIP(ipToUint32(ip) - 1)
}

func broadcastAddr(n *net.IPNet) net.IP {
	ip := n.IP.To4()
	mask := n.Mask
	broadcast := make(net.IP, 4)
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return broadcast
}

func ipCount(rangeStart, rangeEnd net.IP) int {
	return int(ipToUint32(rangeEnd)-ipToUint32(rangeStart)) + 1
}
