/*
Copyright 2019 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	bootstrapv1 "sigs.k8s.io/cluster-api/bootstrap/kubeadm/api/v1alpha3"
	kubeadmv1 "sigs.k8s.io/cluster-api/bootstrap/kubeadm/types/v1beta1"
	"sigs.k8s.io/cluster-api/controllers/external"
	controlplanev1 "sigs.k8s.io/cluster-api/controlplane/kubeadm/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal/hash"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/secret"
)

const (
	// DeleteRequeueAfter is how long to wait before checking again to see if
	// all control plane machines have been deleted.
	DeleteRequeueAfter = 30 * time.Second

	// HealthCheckFailedRequeueAfter is how long to wait before trying to scale
	// up/down if some target cluster health check has failed
	HealthCheckFailedRequeueAfter = 20 * time.Second

	// DependentCertRequeueAfter is how long to wait before checking again to see if
	// dependent certificates have been created.
	DependentCertRequeueAfter = 30 * time.Second
)

// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=roles,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=rolebindings,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io;bootstrap.cluster.x-k8s.io;controlplane.cluster.x-k8s.io,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status,verbs=get;list;watch;create;update;patch;delete

// KubeadmControlPlaneReconciler reconciles a KubeadmControlPlane object
type KubeadmControlPlaneReconciler struct {
	Client     client.Client
	Log        logr.Logger
	scheme     *runtime.Scheme
	controller controller.Controller
	recorder   record.EventRecorder

	managementCluster internal.ManagementCluster
}

func (r *KubeadmControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	c, err := ctrl.NewControllerManagedBy(mgr).
		For(&controlplanev1.KubeadmControlPlane{}).
		Owns(&clusterv1.Machine{}).
		Watches(
			&source.Kind{Type: &clusterv1.Cluster{}},
			&handler.EnqueueRequestsFromMapFunc{ToRequests: handler.ToRequestsFunc(r.ClusterToKubeadmControlPlane)},
		).
		WithOptions(options).
		Build(r)

	if err != nil {
		return errors.Wrap(err, "failed setting up with a controller manager")
	}

	r.scheme = mgr.GetScheme()
	r.controller = c
	r.recorder = mgr.GetEventRecorderFor("kubeadm-control-plane-controller")

	if r.managementCluster == nil {
		r.managementCluster = &internal.Management{Client: r.Client}
	}

	return nil
}

func (r *KubeadmControlPlaneReconciler) Reconcile(req ctrl.Request) (res ctrl.Result, reterr error) {
	logger := r.Log.WithValues("namespace", req.Namespace, "kubeadmControlPlane", req.Name)
	ctx := context.Background()

	// Fetch the KubeadmControlPlane instance.
	kcp := &controlplanev1.KubeadmControlPlane{}
	if err := r.Client.Get(ctx, req.NamespacedName, kcp); err != nil {
		if apierrors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return ctrl.Result{}, nil
		}

		// Error reading the object - requeue the request.
		logger.Error(err, "Failed to retrieve requested KubeadmControlPlane resource from the API Server")
		return ctrl.Result{Requeue: true}, nil
	}

	// Fetch the Cluster.
	cluster, err := util.GetOwnerCluster(ctx, r.Client, kcp.ObjectMeta)
	if err != nil {
		logger.Error(err, "Failed to retrieve owner Cluster from the API Server")
		return ctrl.Result{}, err
	}
	if cluster == nil {
		logger.Info("Cluster Controller has not yet set OwnerRef")
		return ctrl.Result{}, nil
	}
	logger = logger.WithValues("cluster", cluster.Name)

	if util.IsPaused(cluster, kcp) {
		logger.Info("Reconciliation is paused")
		return ctrl.Result{}, nil
	}

	// Wait for the cluster infrastructure to be ready before creating machines
	if !cluster.Status.InfrastructureReady {
		return ctrl.Result{}, nil
	}

	// Initialize the patch helper.
	patchHelper, err := patch.NewHelper(kcp, r.Client)
	if err != nil {
		logger.Error(err, "Failed to configure the patch helper")
		return ctrl.Result{Requeue: true}, nil
	}

	defer func() {
		if requeueErr, ok := errors.Cause(reterr).(capierrors.HasRequeueAfterError); ok {
			if res.RequeueAfter == 0 {
				res.RequeueAfter = requeueErr.GetRequeueAfter()
				reterr = nil
			}
		}
		// Always attempt to update status.
		if err := r.updateStatus(ctx, kcp, cluster); err != nil {
			logger.Error(err, "Failed to update KubeadmControlPlane Status")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// Always attempt to Patch the KubeadmControlPlane object and status after each reconciliation.
		if err := patchHelper.Patch(ctx, kcp); err != nil {
			logger.Error(err, "Failed to patch KubeadmControlPlane")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	if !kcp.ObjectMeta.DeletionTimestamp.IsZero() {
		// Handle deletion reconciliation loop.
		return r.reconcileDelete(ctx, cluster, kcp)
	}

	// Handle normal reconciliation loop.
	return r.reconcile(ctx, cluster, kcp)
}

// reconcile handles KubeadmControlPlane reconciliation.
func (r *KubeadmControlPlaneReconciler) reconcile(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane) (res ctrl.Result, reterr error) {
	logger := r.Log.WithValues("namespace", kcp.Namespace, "kubeadmControlPlane", kcp.Name, "cluster", cluster.Name)

	// If object doesn't have a finalizer, add one.
	controllerutil.AddFinalizer(kcp, controlplanev1.KubeadmControlPlaneFinalizer)

	// Make sure to reconcile the external infrastructure reference.
	if err := r.reconcileExternalReference(ctx, cluster, kcp.Spec.InfrastructureTemplate); err != nil {
		return ctrl.Result{}, err
	}

	// Generate Cluster Certificates if needed
	config := kcp.Spec.KubeadmConfigSpec.DeepCopy()
	config.JoinConfiguration = nil
	if config.ClusterConfiguration == nil {
		config.ClusterConfiguration = &kubeadmv1.ClusterConfiguration{}
	}
	certificates := secret.NewCertificatesForInitialControlPlane(config.ClusterConfiguration)
	controllerRef := metav1.NewControllerRef(kcp, controlplanev1.GroupVersion.WithKind("KubeadmControlPlane"))
	if err := certificates.LookupOrGenerate(ctx, r.Client, util.ObjectKey(cluster), *controllerRef); err != nil {
		logger.Error(err, "unable to lookup or create cluster certificates")
		return ctrl.Result{}, err
	}

	// If ControlPlaneEndpoint is not set, return early
	if cluster.Spec.ControlPlaneEndpoint.IsZero() {
		logger.Info("Cluster does not yet have a ControlPlaneEndpoint defined")
		return ctrl.Result{}, nil
	}

	// Generate Cluster Kubeconfig if needed
	if err := r.reconcileKubeconfig(ctx, util.ObjectKey(cluster), cluster.Spec.ControlPlaneEndpoint, kcp); err != nil {
		logger.Error(err, "failed to reconcile Kubeconfig")
		return ctrl.Result{}, err
	}

	// TODO: handle proper adoption of Machines
	ownedMachines, err := r.managementCluster.GetMachinesForCluster(ctx, util.ObjectKey(cluster), internal.OwnedControlPlaneMachines(kcp.Name))
	if err != nil {
		logger.Error(err, "failed to retrieve control plane machines for cluster")
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	var requireUpgrade internal.FilterableMachineCollection
	if kcp.Spec.UpgradeAfter != nil && kcp.Spec.UpgradeAfter.Before(&now) {
		requireUpgrade = ownedMachines.AnyFilter(
			internal.Not(internal.MatchesConfigurationHash(hash.Compute(&kcp.Spec))),
			internal.OlderThan(kcp.Spec.UpgradeAfter),
		)
	} else {
		requireUpgrade = ownedMachines.Filter(
			internal.Not(internal.MatchesConfigurationHash(hash.Compute(&kcp.Spec))),
		)
	}

	// Upgrade takes precedence over other operations
	if len(requireUpgrade) > 0 {
		logger.Info("Upgrading Control Plane")
		return r.upgradeControlPlane(ctx, cluster, kcp, ownedMachines, requireUpgrade)
	}

	// If we've made it this far, we can assume that all ownedMachines are up to date
	numMachines := len(ownedMachines)
	desiredReplicas := int(*kcp.Spec.Replicas)

	switch {
	// We are creating the first replica
	case numMachines < desiredReplicas && numMachines == 0:
		// Create new Machine w/ init
		logger.Info("Initializing control plane", "Desired", desiredReplicas, "Existing", numMachines)
		return r.initializeControlPlane(ctx, cluster, kcp)
	// We are scaling up
	case numMachines < desiredReplicas && numMachines > 0:
		// Create a new Machine w/ join
		logger.Info("Scaling up control plane", "Desired", desiredReplicas, "Existing", numMachines)
		return r.scaleUpControlPlane(ctx, cluster, kcp, ownedMachines)
	// We are scaling down
	case numMachines > desiredReplicas:
		logger.Info("Scaling down control plane", "Desired", desiredReplicas, "Existing", numMachines)
		return r.scaleDownControlPlane(ctx, cluster, kcp, ownedMachines)
	}

	// Get the workload cluster client.
	workloadCluster, err := r.managementCluster.GetWorkloadCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		// Just log and continue, control-plane cluster may be uninitialized.
		logger.Error(err, "failed to get remote client for workload cluster")
		return ctrl.Result{}, nil
	}

	// Update kube-proxy daemonset.
	if err := workloadCluster.UpdateKubeProxyImageInfo(ctx, kcp); err != nil {
		logger.Error(err, "failed to update kube-proxy image name in kube-proxy daemonset")
		return ctrl.Result{}, err
	}

	// Update CoreDNS deployment.
	if err := workloadCluster.UpdateCoreDNS(ctx, kcp); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to update CoreDNS")
	}

	return ctrl.Result{}, nil
}

func (r *KubeadmControlPlaneReconciler) updateStatus(ctx context.Context, kcp *controlplanev1.KubeadmControlPlane, cluster *clusterv1.Cluster) error {
	labelSelector := internal.ControlPlaneSelectorForCluster(cluster.Name)
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		// Since we are building up the LabelSelector above, this should not fail
		return errors.Wrap(err, "failed to parse label selector")
	}
	// Copy label selector to its status counterpart in string format.
	// This is necessary for CRDs including scale subresources.
	kcp.Status.Selector = selector.String()

	ownedMachines, err := r.managementCluster.GetMachinesForCluster(ctx, util.ObjectKey(cluster), internal.OwnedControlPlaneMachines(kcp.Name))
	if err != nil {
		return errors.Wrap(err, "failed to get list of owned machines")
	}

	currentMachines := ownedMachines.Filter(internal.MatchesConfigurationHash(hash.Compute(&kcp.Spec)))
	kcp.Status.UpdatedReplicas = int32(len(currentMachines))

	replicas := int32(len(ownedMachines))

	// set basic data that does not require interacting with the workload cluster
	kcp.Status.Replicas = replicas
	kcp.Status.ReadyReplicas = 0
	kcp.Status.UnavailableReplicas = replicas

	workloadCluster, err := r.managementCluster.GetWorkloadCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		return errors.Wrap(err, "failed to create remote cluster client")
	}
	status, err := workloadCluster.ClusterStatus(ctx)
	if err != nil {
		return err
	}
	kcp.Status.ReadyReplicas = status.ReadyNodes
	kcp.Status.UnavailableReplicas = replicas - status.ReadyNodes

	// This only gets initialized once and does not change if the kubeadm config map goes away.
	if status.HasKubeadmConfig {
		kcp.Status.Initialized = true
	}

	if kcp.Status.ReadyReplicas > 0 {
		kcp.Status.Ready = true
	}

	return nil
}

func (r *KubeadmControlPlaneReconciler) upgradeControlPlane(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane, ownedMachines internal.FilterableMachineCollection, requireUpgrade internal.FilterableMachineCollection,
) (ctrl.Result, error) {
	logger := r.Log.WithValues("namespace", kcp.Namespace, "kubeadmControlPlane", kcp.Name, "cluster", cluster.Name)

	// TODO: handle reconciliation of etcd members and kubeadm config in case they get out of sync with cluster

	workloadCluster, err := r.managementCluster.GetWorkloadCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		logger.Error(err, "failed to get remote client for workload cluster", "cluster key", util.ObjectKey(cluster))
		return ctrl.Result{}, err
	}

	parsedVersion, err := semver.ParseTolerant(kcp.Spec.Version)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to parse kubernetes version %q", kcp.Spec.Version)
	}

	if err := workloadCluster.ReconcileKubeletRBACRole(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile the remote kubelet RBAC role")
	}

	if err := workloadCluster.ReconcileKubeletRBACBinding(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile the remote kubelet RBAC binding")
	}

	if err := workloadCluster.UpdateKubernetesVersionInKubeadmConfigMap(ctx, kcp.Spec.Version); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to update the kubernetes version in the kubeadm config map")
	}

	if kcp.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local != nil {
		meta := kcp.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local.ImageMeta
		if err := workloadCluster.UpdateEtcdVersionInKubeadmConfigMap(ctx, meta.ImageRepository, meta.ImageTag); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update the etcd version in the kubeadm config map")
		}
	}

	if err := workloadCluster.UpdateKubeletConfigMap(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to upgrade kubelet config map")
	}

	// If there is not already a Machine that is marked for upgrade, find one and mark it
	selectedForUpgrade := requireUpgrade.Filter(internal.HasAnnotationKey(controlplanev1.SelectedForUpgradeAnnotation))
	if len(selectedForUpgrade) == 0 {
		selectedMachine, err := r.selectMachineForUpgrade(ctx, cluster, requireUpgrade)
		if err != nil {
			logger.Error(err, "failed to select machine for upgrade")
			return ctrl.Result{}, err
		}
		selectedForUpgrade = selectedForUpgrade.Insert(selectedMachine)
	}

	replacementCreated := selectedForUpgrade.Filter(internal.HasAnnotationKey(controlplanev1.UpgradeReplacementCreatedAnnotation))
	if len(replacementCreated) == 0 {
		// TODO: should we also add a check here to ensure that current machines not > kcp.spec.replicas+1?
		// We haven't created a replacement machine for the cluster yet
		// return here to avoid blocking while waiting for the new control plane Machine to come up
		result, err := r.scaleUpControlPlane(ctx, cluster, kcp, ownedMachines)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.markWithAnnotationKey(ctx, selectedForUpgrade.Oldest(), controlplanev1.UpgradeReplacementCreatedAnnotation); err != nil {
			return ctrl.Result{}, err
		}
		return result, nil
	}

	return r.scaleDownControlPlane(ctx, cluster, kcp, replacementCreated)
}

func (r *KubeadmControlPlaneReconciler) markWithAnnotationKey(ctx context.Context, machine *clusterv1.Machine, annotationKey string) error {
	if machine == nil {
		return errors.New("expected machine not nil")
	}
	patchHelper, err := patch.NewHelper(machine, r.Client)
	if err != nil {
		return errors.Wrapf(err, "failed to create patch helper for machine %s", machine.Name)
	}

	if machine.Annotations == nil {
		machine.Annotations = make(map[string]string)
	}
	machine.Annotations[annotationKey] = ""

	if err := patchHelper.Patch(ctx, machine); err != nil {
		return errors.Wrapf(err, "failed to patch machine %s selected for upgrade", machine.Name)
	}
	return nil
}

func (r *KubeadmControlPlaneReconciler) selectMachineForUpgrade(ctx context.Context, cluster *clusterv1.Cluster, requireUpgrade internal.FilterableMachineCollection) (*clusterv1.Machine, error) {
	failureDomain := r.failureDomainForScaleDown(cluster, requireUpgrade)

	inFailureDomain := requireUpgrade.Filter(internal.InFailureDomains(failureDomain))
	selected := inFailureDomain.Oldest()

	if err := r.markWithAnnotationKey(ctx, selected, controlplanev1.SelectedForUpgradeAnnotation); err != nil {
		return nil, errors.Wrap(err, "failed to select and mark a machine for upgrade")
	}

	return selected, nil
}

func (r *KubeadmControlPlaneReconciler) initializeControlPlane(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane) (ctrl.Result, error) {
	logger := r.Log.WithValues("namespace", kcp.Namespace, "kubeadmControlPlane", kcp.Name, "cluster", cluster.Name)

	bootstrapSpec := kcp.Spec.KubeadmConfigSpec.DeepCopy()
	bootstrapSpec.JoinConfiguration = nil

	fd := r.failureDomainForScaleUp(cluster, nil)
	if err := r.cloneConfigsAndGenerateMachine(ctx, cluster, kcp, bootstrapSpec, fd); err != nil {
		logger.Error(err, "failed to create initial control plane Machine")
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "FailedInitialization", "Failed to create initial control plane Machine for cluster %s/%s control plane: %v", cluster.Namespace, cluster.Name, err)
		return ctrl.Result{}, err
	}

	// Requeue the control plane, in case there are additional operations to perform
	return ctrl.Result{Requeue: true}, nil
}

func (r *KubeadmControlPlaneReconciler) scaleUpControlPlane(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane, machines internal.FilterableMachineCollection) (ctrl.Result, error) {
	logger := r.Log.WithValues("namespace", kcp.Namespace, "kubeadmControlPlane", kcp.Name, "cluster", cluster.Name)
	if err := r.managementCluster.TargetClusterControlPlaneIsHealthy(ctx, util.ObjectKey(cluster), kcp.Name); err != nil {
		logger.Error(err, "waiting for control plane to pass control plane health check before adding an additional control plane machine")
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "ControlPlaneUnhealthy", "Waiting for control plane to pass control plane health check before adding additional control plane machine: %v", err)
		return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: HealthCheckFailedRequeueAfter}
	}

	if err := r.managementCluster.TargetClusterEtcdIsHealthy(ctx, util.ObjectKey(cluster), kcp.Name); err != nil {
		logger.Error(err, "waiting for control plane to pass etcd health check before adding an additional control plane machine")
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "ControlPlaneUnhealthy", "Waiting for control plane to pass etcd health check before adding additional control plane machine: %v", err)
		return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: HealthCheckFailedRequeueAfter}
	}

	// Create the bootstrap configuration
	bootstrapSpec := kcp.Spec.KubeadmConfigSpec.DeepCopy()
	bootstrapSpec.InitConfiguration = nil
	bootstrapSpec.ClusterConfiguration = nil

	fd := r.failureDomainForScaleUp(cluster, machines)
	if err := r.cloneConfigsAndGenerateMachine(ctx, cluster, kcp, bootstrapSpec, fd); err != nil {
		logger.Error(err, "failed to create additional control plane Machine")
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "FailedScaleUp", "Failed to create additional control plane Machine for cluster %s/%s control plane: %v", cluster.Namespace, cluster.Name, err)
		return ctrl.Result{}, err
	}

	// Requeue the control plane, in case there are other operations to perform
	return ctrl.Result{Requeue: true}, nil
}

func (r *KubeadmControlPlaneReconciler) scaleDownControlPlane(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane, machines internal.FilterableMachineCollection) (ctrl.Result, error) {
	logger := r.Log.WithValues("namespace", kcp.Namespace, "kubeadmControlPlane", kcp.Name, "cluster", cluster.Name)

	workloadCluster, err := r.managementCluster.GetWorkloadCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		logger.Error(err, "failed to create client to workload cluster")
		return ctrl.Result{}, errors.New("failed to create client to workload cluster")
	}

	// We don't want to health check at the beginning of this method to avoid blocking re-entrancy

	// Wait for any delete in progress to complete before deleting another Machine
	if len(machines.Filter(internal.HasDeletionTimestamp)) > 0 {
		return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: DeleteRequeueAfter}
	}

	markedForDeletion := machines.Filter(internal.HasAnnotationKey(controlplanev1.DeleteForScaleDownAnnotation))
	if len(markedForDeletion) == 0 {
		fd := r.failureDomainForScaleDown(cluster, machines)
		machinesInFailureDomain := machines.Filter(internal.InFailureDomains(fd))
		machineToMark := machinesInFailureDomain.Oldest()
		if machineToMark == nil {
			logger.Info("failed to pick control plane Machine to mark for deletion")
			return ctrl.Result{}, errors.New("failed to pick control plane Machine to mark for deletion")
		}
		if err := r.markWithAnnotationKey(ctx, machineToMark, controlplanev1.DeleteForScaleDownAnnotation); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to mark machine %s for deletion", machineToMark.Name)
		}
		markedForDeletion.Insert(machinesInFailureDomain.Oldest())
	}

	machineToDelete := markedForDeletion.Oldest()
	if machineToDelete == nil {
		logger.Info("failed to pick control plane Machine to delete")
		return ctrl.Result{}, errors.New("failed to pick control plane Machine to delete")
	}

	if !internal.HasAnnotationKey(controlplanev1.ScaleDownEtcdMemberRemovedAnnotation)(machineToDelete) {
		// Ensure etcd is healthy prior to attempting to remove the member
		if err := r.managementCluster.TargetClusterEtcdIsHealthy(ctx, util.ObjectKey(cluster), kcp.Name); err != nil {
			logger.Error(err, "waiting for control plane to pass etcd health check before removing a control plane machine")
			r.recorder.Eventf(kcp, corev1.EventTypeWarning, "ControlPlaneUnhealthy", "Waiting for control plane to pass etcd health check before removing a control plane machine: %v", err)
			return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: HealthCheckFailedRequeueAfter}
		}
		if err := workloadCluster.RemoveEtcdMemberForMachine(ctx, machineToDelete); err != nil {
			logger.Error(err, "failed to remove etcd member for machine")
			return ctrl.Result{}, err
		}
		if err := r.markWithAnnotationKey(ctx, machineToDelete, controlplanev1.ScaleDownEtcdMemberRemovedAnnotation); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to mark machine %s as having etcd membership removed", machineToDelete.Name)
		}
	}

	if !internal.HasAnnotationKey(controlplanev1.ScaleDownConfigMapEntryRemovedAnnotation)(machineToDelete) {
		if err := r.managementCluster.TargetClusterControlPlaneIsHealthy(ctx, util.ObjectKey(cluster), kcp.Name); err != nil {
			logger.Error(err, "waiting for control plane to pass control plane health check before removing a control plane machine")
			r.recorder.Eventf(kcp, corev1.EventTypeWarning, "ControlPlaneUnhealthy", "Waiting for control plane to pass control plane health check before removing a control plane machine: %v", err)
			return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: HealthCheckFailedRequeueAfter}

		}
		if err := workloadCluster.RemoveMachineFromKubeadmConfigMap(ctx, machineToDelete); err != nil {
			logger.Error(err, "failed to remove machine from kubeadm ConfigMap")
			return ctrl.Result{}, err
		}
		if err := r.markWithAnnotationKey(ctx, machineToDelete, controlplanev1.ScaleDownConfigMapEntryRemovedAnnotation); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to mark machine %s as having config map entry removed", machineToDelete.Name)
		}
	}

	// Do a final health check of the Control Plane components prior to actually deleting the machine
	if err := r.managementCluster.TargetClusterControlPlaneIsHealthy(ctx, util.ObjectKey(cluster), kcp.Name); err != nil {
		logger.Error(err, "waiting for control plane to pass control plane health check before removing a control plane machine")
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "ControlPlaneUnhealthy", "Waiting for control plane to pass control plane health check before removing a control plane machine: %v", err)
		return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: HealthCheckFailedRequeueAfter}
	}
	logger = logger.WithValues("machine", machineToDelete)
	if err := r.Client.Delete(ctx, machineToDelete); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete control plane machine")
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "FailedScaleDown", "Failed to delete control plane Machine %s for cluster %s/%s control plane: %v", machineToDelete.Name, cluster.Namespace, cluster.Name, err)
		return ctrl.Result{}, err
	}

	// Requeue the control plane, in case there are additional operations to perform
	return ctrl.Result{Requeue: true}, nil
}

func (r *KubeadmControlPlaneReconciler) cloneConfigsAndGenerateMachine(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane, bootstrapSpec *bootstrapv1.KubeadmConfigSpec, failureDomain *string) error {
	var errs []error

	// Since the cloned resource should eventually have a controller ref for the Machine, we create an
	// OwnerReference here without the Controller field set
	infraCloneOwner := &metav1.OwnerReference{
		APIVersion: controlplanev1.GroupVersion.String(),
		Kind:       "KubeadmControlPlane",
		Name:       kcp.Name,
		UID:        kcp.UID,
	}

	// Clone the infrastructure template
	infraRef, err := external.CloneTemplate(ctx, &external.CloneTemplateInput{
		Client:      r.Client,
		TemplateRef: &kcp.Spec.InfrastructureTemplate,
		Namespace:   kcp.Namespace,
		OwnerRef:    infraCloneOwner,
		ClusterName: cluster.Name,
		Labels:      internal.ControlPlaneLabelsForClusterWithHash(cluster.Name, hash.Compute(&kcp.Spec)),
	})
	if err != nil {
		// Safe to return early here since no resources have been created yet.
		return errors.Wrap(err, "failed to clone infrastructure template")
	}

	// Clone the bootstrap configuration
	bootstrapRef, err := r.generateKubeadmConfig(ctx, kcp, cluster, bootstrapSpec)
	if err != nil {
		errs = append(errs, errors.Wrap(err, "failed to generate bootstrap config"))
	}

	// Only proceed to generating the Machine if we haven't encountered an error
	if len(errs) == 0 {
		if err := r.generateMachine(ctx, kcp, cluster, infraRef, bootstrapRef, failureDomain); err != nil {
			errs = append(errs, errors.Wrap(err, "failed to create Machine"))
		}
	}

	// If we encountered any errors, attempt to clean up any dangling resources
	if len(errs) > 0 {
		if err := r.cleanupFromGeneration(ctx, infraRef, bootstrapRef); err != nil {
			errs = append(errs, errors.Wrap(err, "failed to cleanup generated resources"))
		}

		return kerrors.NewAggregate(errs)
	}

	return nil
}

func (r *KubeadmControlPlaneReconciler) cleanupFromGeneration(ctx context.Context, remoteRefs ...*corev1.ObjectReference) error {
	var errs []error

	for _, ref := range remoteRefs {
		if ref != nil {
			config := &unstructured.Unstructured{}
			config.SetKind(ref.Kind)
			config.SetAPIVersion(ref.APIVersion)
			config.SetNamespace(ref.Namespace)
			config.SetName(ref.Name)

			if err := r.Client.Delete(ctx, config); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, errors.Wrap(err, "failed to cleanup generated resources after error"))
			}
		}
	}

	return kerrors.NewAggregate(errs)
}

func (r *KubeadmControlPlaneReconciler) generateKubeadmConfig(ctx context.Context, kcp *controlplanev1.KubeadmControlPlane, cluster *clusterv1.Cluster, spec *bootstrapv1.KubeadmConfigSpec) (*corev1.ObjectReference, error) {
	// Create an owner reference without a controller reference because the owning controller is the machine controller
	owner := metav1.OwnerReference{
		APIVersion: controlplanev1.GroupVersion.String(),
		Kind:       "KubeadmControlPlane",
		Name:       kcp.Name,
		UID:        kcp.UID,
	}

	bootstrapConfig := &bootstrapv1.KubeadmConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:            names.SimpleNameGenerator.GenerateName(kcp.Name + "-"),
			Namespace:       kcp.Namespace,
			Labels:          internal.ControlPlaneLabelsForClusterWithHash(cluster.Name, hash.Compute(&kcp.Spec)),
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: *spec,
	}

	if err := r.Client.Create(ctx, bootstrapConfig); err != nil {
		return nil, errors.Wrap(err, "Failed to create bootstrap configuration")
	}

	bootstrapRef := &corev1.ObjectReference{
		APIVersion: bootstrapv1.GroupVersion.String(),
		Kind:       "KubeadmConfig",
		Name:       bootstrapConfig.GetName(),
		Namespace:  bootstrapConfig.GetNamespace(),
		UID:        bootstrapConfig.GetUID(),
	}

	return bootstrapRef, nil
}

func (r *KubeadmControlPlaneReconciler) failureDomainForScaleDown(cluster *clusterv1.Cluster, machines internal.FilterableMachineCollection) *string {
	// See if there are any Machines that are not in currently defined failure domains first.
	notInFailureDomains := machines.Filter(internal.Not(internal.InFailureDomains(cluster.Status.FailureDomains.FilterControlPlane().GetIDs()...)))
	if len(notInFailureDomains) > 0 {
		// return the failure domain for the oldest Machine not in the current list of failure domains
		// this could be either nil (no failure domain defined) or a failure domain that is no longer defined
		// in the cluster status.
		return notInFailureDomains.Oldest().Spec.FailureDomain
	}

	// Otherwise pick the currently known failure domain with the most Machines
	return internal.PickMost(cluster.Status.FailureDomains.FilterControlPlane(), machines)
}

func (r *KubeadmControlPlaneReconciler) failureDomainForScaleUp(cluster *clusterv1.Cluster, machines internal.FilterableMachineCollection) *string {
	// Don't do anything if there are no failure domains defined on the cluster.
	if len(cluster.Status.FailureDomains.FilterControlPlane()) == 0 {
		return nil
	}
	return internal.PickFewest(cluster.Status.FailureDomains.FilterControlPlane(), machines)
}

func (r *KubeadmControlPlaneReconciler) generateMachine(ctx context.Context, kcp *controlplanev1.KubeadmControlPlane, cluster *clusterv1.Cluster, infraRef, bootstrapRef *corev1.ObjectReference, failureDomain *string) error {
	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.SimpleNameGenerator.GenerateName(kcp.Name + "-"),
			Namespace: kcp.Namespace,
			Labels:    internal.ControlPlaneLabelsForClusterWithHash(cluster.Name, hash.Compute(&kcp.Spec)),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(kcp, controlplanev1.GroupVersion.WithKind("KubeadmControlPlane")),
			},
		},
		Spec: clusterv1.MachineSpec{
			ClusterName:       cluster.Name,
			Version:           &kcp.Spec.Version,
			InfrastructureRef: *infraRef,
			Bootstrap: clusterv1.Bootstrap{
				ConfigRef: bootstrapRef,
			},
			FailureDomain: failureDomain,
		},
	}

	if err := r.Client.Create(ctx, machine); err != nil {
		return errors.Wrap(err, "Failed to create machine")
	}

	return nil
}

// reconcileDelete handles KubeadmControlPlane deletion.
// The implementation does not take non-control plane workloads into
// consideration. This may or may not change in the future. Please see
// https://github.com/kubernetes-sigs/cluster-api/issues/2064
func (r *KubeadmControlPlaneReconciler) reconcileDelete(ctx context.Context, cluster *clusterv1.Cluster, kcp *controlplanev1.KubeadmControlPlane) (_ ctrl.Result, reterr error) {
	logger := r.Log.WithValues("namespace", kcp.Namespace, "kubeadmControlPlane", kcp.Name, "cluster", cluster.Name)
	allMachines, err := r.managementCluster.GetMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		logger.Error(err, "failed to retrieve machines for cluster")
		return ctrl.Result{}, err
	}
	ownedMachines := allMachines.Filter(internal.OwnedControlPlaneMachines(kcp.Name))

	// If no control plane machines remain, remove the finalizer
	if len(ownedMachines) == 0 {
		controllerutil.RemoveFinalizer(kcp, controlplanev1.KubeadmControlPlaneFinalizer)
		return ctrl.Result{}, nil
	}

	// Verify that only control plane machines remain
	if len(allMachines) != len(ownedMachines) {
		logger.Info("Non control plane machines exist and must be removed before control plane machines are removed")
		return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: DeleteRequeueAfter}
	}

	// Delete control plane machines in parallel
	machinesToDelete := ownedMachines.Filter(internal.Not(internal.HasDeletionTimestamp))
	var errs []error
	for i := range machinesToDelete {
		m := machinesToDelete[i]
		logger := logger.WithValues("machine", m)
		if err := r.Client.Delete(ctx, machinesToDelete[i]); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to cleanup owned machine")
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		err := kerrors.NewAggregate(errs)
		r.recorder.Eventf(kcp, corev1.EventTypeWarning, "FailedDelete", "Failed to delete control plane Machines for cluster %s/%s control plane: %v", cluster.Namespace, cluster.Name, err)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, &capierrors.RequeueAfterError{RequeueAfter: DeleteRequeueAfter}
}

func (r *KubeadmControlPlaneReconciler) reconcileKubeconfig(ctx context.Context, clusterName client.ObjectKey, endpoint clusterv1.APIEndpoint, kcp *controlplanev1.KubeadmControlPlane) error {
	if endpoint.IsZero() {
		return nil
	}

	_, err := secret.GetFromNamespacedName(ctx, r.Client, clusterName, secret.Kubeconfig)
	switch {
	case apierrors.IsNotFound(err):
		createErr := kubeconfig.CreateSecretWithOwner(
			ctx,
			r.Client,
			clusterName,
			endpoint.String(),
			*metav1.NewControllerRef(kcp, controlplanev1.GroupVersion.WithKind("KubeadmControlPlane")),
		)
		if createErr != nil {
			if createErr == kubeconfig.ErrDependentCertificateNotFound {
				return errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: DependentCertRequeueAfter},
					"could not find secret %q for Cluster %q in namespace %q, requeuing",
					secret.ClusterCA, clusterName.Name, clusterName.Namespace)
			}
			return createErr
		}
	case err != nil:
		return errors.Wrapf(err, "failed to retrieve Kubeconfig Secret for Cluster %q in namespace %q", clusterName.Name, clusterName.Namespace)
	}

	return nil
}

func (r *KubeadmControlPlaneReconciler) reconcileExternalReference(ctx context.Context, cluster *clusterv1.Cluster, ref corev1.ObjectReference) error {
	if !strings.HasSuffix(ref.Kind, external.TemplateSuffix) {
		return nil
	}

	obj, err := external.Get(ctx, r.Client, &ref, cluster.Namespace)
	if err != nil {
		return err
	}

	// Note: We intentionally do not handle checking for the paused label on an external template reference

	patchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return err
	}

	obj.SetOwnerReferences(util.EnsureOwnerRef(obj.GetOwnerReferences(), metav1.OwnerReference{
		APIVersion: clusterv1.GroupVersion.String(),
		Kind:       "Cluster",
		Name:       cluster.Name,
		UID:        cluster.UID,
	}))

	if err := patchHelper.Patch(ctx, obj); err != nil {
		return err
	}
	return nil
}

// ClusterToKubeadmControlPlane is a handler.ToRequestsFunc to be used to enqueue requests for reconciliation
// for KubeadmControlPlane based on updates to a Cluster.
func (r *KubeadmControlPlaneReconciler) ClusterToKubeadmControlPlane(o handler.MapObject) []ctrl.Request {
	c, ok := o.Object.(*clusterv1.Cluster)
	if !ok {
		r.Log.Error(nil, fmt.Sprintf("Expected a Cluster but got a %T", o.Object))
		return nil
	}

	controlPlaneRef := c.Spec.ControlPlaneRef
	if controlPlaneRef != nil && controlPlaneRef.Kind == "KubeadmControlPlane" {
		name := client.ObjectKey{Namespace: controlPlaneRef.Namespace, Name: controlPlaneRef.Name}
		return []ctrl.Request{{NamespacedName: name}}
	}

	return nil
}
