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

package machine

import (
	"context"
	"path"
	"time"

	"k8s.io/klog"
	"k8s.io/utils/pointer"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha2"
	clusterv1 "sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha2"
	capierrors "sigs.k8s.io/cluster-api/pkg/controller/error"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type phaseReconcilerOutput struct {
	Changed bool
	Error   error
}

func (r *ReconcileMachine) reconcile(ctx context.Context, m *v1alpha2.Machine) (bool, error) {
	// TODO(vincepri): The two following methods are *very* similar to each other and they can probably
	// be generalized into smaller methods, reducing their complexity. That said, given the complexity
	// of this state machine, we'll leave this task as a future improvement.
	bootstrapOut := r.reconcileBootstrap(ctx, m)
	infrastructureOut := r.reconcileInfrastructure(ctx, m)

	// Store the initial phase to determine if it has changed.
	initialPhase := m.Status.GetTypedPhase()

	// Set the phase to "pending" if nil.
	if m.Status.Phase == nil {
		m.Status.SetTypedPhase(v1alpha2.MachinePhasePending)
	}

	// Set the phase to "provisioning" if bootstrap is ready and the infrastructure isn't.
	if m.Status.BootstrapReady && !m.Status.InfrastructureReady {
		m.Status.SetTypedPhase(v1alpha2.MachinePhaseProvisioning)
	}

	// Set the phase to "provisioned" if the infrastructure is ready.
	if m.Status.InfrastructureReady {
		m.Status.SetTypedPhase(v1alpha2.MachinePhaseProvisioned)
	}

	// Set the phase to "running" if there is a NodeRef field.
	if m.Status.NodeRef != nil && m.Status.InfrastructureReady {
		m.Status.SetTypedPhase(v1alpha2.MachinePhaseRunning)
	}

	// Set the phase to "deleting" if the deletion timestamp is set.
	if !m.DeletionTimestamp.IsZero() {
		m.Status.SetTypedPhase(v1alpha2.MachinePhaseDeleting)
	}

	// Set the phase to "deleted" if the deletion timestamp is set
	// and both infrastructure and bootstrap aren't ready.
	if !m.DeletionTimestamp.IsZero() && !m.Status.BootstrapReady && !m.Status.InfrastructureReady && len(m.Finalizers) == 0 {
		m.Status.SetTypedPhase(v1alpha2.MachinePhaseDeleted)
	}

	// Determine if something has changed and the caller has to perform updates.
	changed := bootstrapOut.Changed || infrastructureOut.Changed || m.Status.GetTypedPhase() != initialPhase

	// Determine the return error, giving precedence to non-nil errors and non-requeueAfter.
	var err error
	if bootstrapOut.Error != nil {
		err = bootstrapOut.Error
	} else if infrastructureOut.Error != nil && (err == nil || capierrors.IsRequeueAfter(err)) {
		err = infrastructureOut.Error
	}

	return changed, err
}

func (r *ReconcileMachine) reconcileExternal(ctx context.Context, m *v1alpha2.Machine, ref *corev1.ObjectReference) (*unstructured.Unstructured, error) {
	external := new(unstructured.Unstructured)
	external.SetAPIVersion(ref.APIVersion)
	external.SetKind(ref.Kind)
	external.SetName(ref.Name)
	external.SetNamespace(m.Namespace)

	// Retrieve external object.
	if err := r.getUnstructured(ctx, external); err != nil {
		if apierrors.IsNotFound(err) && !m.DeletionTimestamp.IsZero() {
			return nil, nil
		} else if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: 30 * time.Second},
				"could not find %s %q for Machine %q in namespace %q, requeuing",
				path.Join(ref.APIVersion, ref.Kind), ref.Name, m.Name, m.Namespace)
		}
		return nil, err
	}

	// Delete the external object if the Machine is being deleted.
	if !m.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, external); err != nil {
			return nil, errors.Wrapf(err,
				"failed to delete %s %q for Machine %q in namespace %q",
				path.Join(ref.APIVersion, ref.Kind), ref.Name, m.Name, m.Namespace)
		}
		return external, nil
	}

	// Set external object OwnerReference to the Machine.
	if len(external.GetOwnerReferences()) == 0 {
		if err := r.setUnstructuredOwnerRef(ctx, m, external); err != nil {
			return nil, errors.Wrapf(err,
				"failed to set OwnerReference on %s %q for Machine %q in namespace %q",
				path.Join(ref.APIVersion, ref.Kind), ref.Name, m.Name, m.Namespace)
		}
	}

	return external, nil
}

func (r *ReconcileMachine) isExternalReady(ref *unstructured.Unstructured) (bool, error) {
	ready, found, err := unstructured.NestedBool(ref.Object, "status", "ready")
	if err != nil {
		return false, errors.Wrapf(err, "failed to determine %s %q readiness", path.Join(ref.GetAPIVersion(), ref.GetKind()), ref.GetName())
	}
	return ready && found, nil
}

func (r *ReconcileMachine) reconcileBootstrap(ctx context.Context, m *v1alpha2.Machine) phaseReconcilerOutput {
	// Keep the initial readiness status around to determine changes.
	wasReady := m.Status.BootstrapReady

	if m.Spec.Bootstrap.ConfigRef == nil && m.Spec.Bootstrap.Data == nil {
		return phaseReconcilerOutput{
			Error: errors.Errorf(
				"Expected at least one of `Bootstrap.ConfigRef` or `Bootstrap.Data` to be populated for Machine %q in namespace %q",
				m.Name, m.Namespace,
			),
		}
	} else if m.Spec.Bootstrap.ConfigRef == nil {
		m.Status.BootstrapReady = true
		return phaseReconcilerOutput{Changed: !wasReady && m.Status.BootstrapReady}
	}

	bootstrapConfig, err := r.reconcileExternal(ctx, m, m.Spec.Bootstrap.ConfigRef)
	if bootstrapConfig == nil && err == nil {
		m.Status.BootstrapReady = false
		return phaseReconcilerOutput{Changed: wasReady && !m.Status.BootstrapReady}
	} else if err != nil {
		return phaseReconcilerOutput{Error: err}
	}

	if !bootstrapConfig.GetDeletionTimestamp().IsZero() {
		return phaseReconcilerOutput{}
	}

	// Determine if the bootstrap provider is ready.
	ready, err := r.isExternalReady(bootstrapConfig)
	if err != nil {
		return phaseReconcilerOutput{Error: err}
	} else if !ready {
		klog.V(3).Infof("Bootstrap provider for Machine %q in namespace %q is not ready, requeuing", m.Name, m.Namespace)
		m.Status.BootstrapReady = false
		return phaseReconcilerOutput{
			Changed: wasReady && !m.Status.BootstrapReady,
			Error:   &capierrors.RequeueAfterError{RequeueAfter: 30 * time.Second},
		}
	}

	// Get and set data from the bootstrap provider.
	if m.Spec.Bootstrap.Data == nil {
		data, _, err := unstructured.NestedString(bootstrapConfig.Object, "status", "bootstrapData")
		if err != nil {
			return phaseReconcilerOutput{
				Error: errors.Wrapf(err, "failed to retrieve data from bootstrap provider for Machine %q in namespace %q", m.Name, m.Namespace),
			}
		} else if data == "" {
			return phaseReconcilerOutput{
				Error: errors.Errorf("retrieved empty data from bootstrap provider for Machine %q in namespace %q", m.Name, m.Namespace),
			}
		}

		m.Spec.Bootstrap.Data = pointer.StringPtr(data)
	}

	m.Status.BootstrapReady = true
	return phaseReconcilerOutput{
		Changed: !wasReady && m.Status.BootstrapReady,
	}
}

func (r *ReconcileMachine) reconcileInfrastructure(ctx context.Context, m *v1alpha2.Machine) phaseReconcilerOutput {
	// Keep the initial readiness status around to determine changes.
	wasReady := m.Status.InfrastructureReady

	infraConfig, err := r.reconcileExternal(ctx, m, &m.Spec.InfrastructureRef)
	if infraConfig == nil && err == nil {
		m.Status.InfrastructureReady = false
		return phaseReconcilerOutput{Changed: wasReady && !m.Status.InfrastructureReady}
	} else if err != nil {
		return phaseReconcilerOutput{Error: err}
	}

	if !infraConfig.GetDeletionTimestamp().IsZero() {
		return phaseReconcilerOutput{}
	}

	// Determine if the infrastructure provider is ready
	ready, err := r.isExternalReady(infraConfig)
	if err != nil {
		return phaseReconcilerOutput{Error: err}
	} else if !ready {
		klog.V(3).Infof("Infrastructure provider for Machine %q in namespace %q is not ready, requeuing", m.Name, m.Namespace)
		m.Status.InfrastructureReady = false
		return phaseReconcilerOutput{
			Changed: wasReady && !m.Status.InfrastructureReady,
			Error:   &capierrors.RequeueAfterError{RequeueAfter: 30 * time.Second},
		}
	}

	// Get addresses.
	addresses, _, err := unstructured.NestedSlice(infraConfig.Object, "status", "addresses")
	if err != nil {
		return phaseReconcilerOutput{
			Error: errors.Wrapf(err, "failed to retrieve addresses from infrastructure provider for Machine %q in namespace %q", m.Name, m.Namespace),
		}
	} else if len(addresses) == 0 {
		return phaseReconcilerOutput{
			Error: errors.Errorf("infrastructure provider returned ready and no addresses for Machine %q in namespace %q", m.Name, m.Namespace),
		}
	} else {
		m.Status.InfrastructureReady = true
	}

	// Communicate a change only if the readiness field has changed from the last run.
	return phaseReconcilerOutput{
		Changed: !wasReady && m.Status.InfrastructureReady,
	}
}

func (r *ReconcileMachine) getUnstructured(ctx context.Context, obj *unstructured.Unstructured) error {
	key := client.ObjectKey{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	return r.Get(ctx, key, obj)
}

func (r *ReconcileMachine) setUnstructuredOwnerRef(ctx context.Context, m *clusterv1.Machine, obj *unstructured.Unstructured) error {
	obj.SetOwnerReferences([]metav1.OwnerReference{
		metav1.OwnerReference{
			APIVersion: m.APIVersion,
			Kind:       m.Kind,
			Name:       m.Name,
		},
	})

	return r.Update(ctx, obj)
}
