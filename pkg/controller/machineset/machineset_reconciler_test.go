/*
Copyright 2018 The Kubernetes Authors.

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

package machineset

import (
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"golang.org/x/net/context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	clusterv1alpha2 "sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const timeout = time.Second * 5

func TestReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	ctx := context.TODO()

	expectedRequest := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "foo",
			Namespace: "default",
		},
	}

	replicas := int32(2)
	version := "1.14.2"
	instance := &clusterv1alpha2.MachineSet{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "default"},
		Spec: clusterv1alpha2.MachineSetSpec{
			Replicas: &replicas,
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"label-1": "true",
				},
			},
			Template: clusterv1alpha2.MachineTemplateSpec{
				ObjectMeta: clusterv1alpha2.ObjectMeta{
					Labels: map[string]string{
						"label-1": "true",
					},
				},
				Spec: clusterv1alpha2.MachineSpec{
					Version: &version,
					InfrastructureRef: corev1.ObjectReference{
						APIVersion: "infrastructure.cluster.sigs.k8s.io/v1alpha1",
						Kind:       "InfrastructureRef",
						Name:       "foo-template",
					},
				},
			},
		},
	}

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(cfg, manager.Options{})
	g.Expect(err).To(gomega.BeNil())
	c := mgr.GetClient()

	// Create infrastructure template resource.
	infraResource := new(unstructured.Unstructured)
	infraResource.SetKind("InfrastructureRef")
	infraResource.SetAPIVersion("infrastructure.cluster.sigs.k8s.io/v1alpha1")
	infraResource.SetName("foo-template")
	infraResource.SetNamespace("default")
	g.Expect(c.Create(ctx, infraResource)).To(gomega.BeNil())

	r := newReconciler(mgr)
	recFn, requests := SetupTestReconcile(r)
	if err := add(mgr, recFn, r.MachineToMachineSets); err != nil {
		t.Errorf("error adding controller to manager: %v", err)
	}
	defer close(StartTestManager(mgr, t))

	// Create the MachineSet object and expect Reconcile to be called and the Machines to be created.
	g.Expect(c.Create(ctx, instance)).To(gomega.BeNil())
	defer c.Delete(ctx, instance)
	select {
	case recv := <-requests:
		if recv != expectedRequest {
			t.Error("received request does not match expected request")
		}
	case <-time.After(timeout):
		t.Error("timed out waiting for request")
	}

	machines := &clusterv1alpha2.MachineList{}

	// Verify that we have 2 replicas.
	g.Eventually(func() int {
		if err := c.List(ctx, machines); err != nil {
			return -1
		}
		return len(machines.Items)
	}, timeout).Should(gomega.BeEquivalentTo(replicas))

	// Verify that each machine has the desired kubelet version.
	for _, m := range machines.Items {
		g.Expect(m.Spec.Version).ToNot(gomega.BeNil())
		g.Expect(*m.Spec.Version).To(gomega.BeEquivalentTo("1.14.2"))

		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-",
			},
			Spec: corev1.NodeSpec{
				ProviderID: fmt.Sprintf("test:///%s", m.Name),
			},
		}

		g.Expect(c.Create(ctx, node))
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: corev1.NodeReady, Status: "True"})
		g.Expect(c.Status().Update(ctx, node)).To(gomega.BeNil())

		m.Spec.ProviderID = pointer.StringPtr(node.Spec.ProviderID)
		m.Status.NodeRef = &corev1.ObjectReference{
			APIVersion: node.APIVersion,
			Kind:       node.Kind,
			Name:       node.Name,
			UID:        node.UID,
		}
		g.Expect(c.Status().Update(ctx, &m)).To(gomega.BeNil())
	}

	// Verify that we have 3 infrastructure references: 1 template + 2 machines.
	infraConfigs := &unstructured.UnstructuredList{}
	infraConfigs.SetKind("InfrastructureRef")
	infraConfigs.SetAPIVersion("infrastructure.cluster.sigs.k8s.io/v1alpha1")
	g.Eventually(func() int {
		if err := c.List(ctx, infraConfigs); err != nil {
			return -1
		}
		return len(infraConfigs.Items)
	}, timeout).Should(gomega.BeEquivalentTo(1 + replicas))

	m := machines.Items[0]
	g.Expect(c.Delete(ctx, &m)).To(gomega.BeNil())
	select {
	case recv := <-requests:
		if recv != expectedRequest {
			t.Error("received request does not match expected request")
		}
	case <-time.After(timeout):
		t.Error("timed out waiting for request")
	}
	g.Eventually(func() int {
		if err := c.List(ctx, machines); err != nil {
			return -1
		}
		return len(machines.Items)
	}, timeout).Should(gomega.BeEquivalentTo(replicas))
	g.Eventually(func() int {
		if err := c.List(ctx, infraConfigs); err != nil {
			return -1
		}
		return len(infraConfigs.Items)
	}, timeout).Should(gomega.BeEquivalentTo(1 + replicas))
}

func expectInt(t *testing.T, expect int, fn func(context.Context) int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	intCh := make(chan int)

	go func() { intCh <- fn(ctx) }()

	select {
	case n := <-intCh:
		if n != expect {
			t.Errorf("go unexpectef value %d, expected %d", n, expect)
		}
	case <-ctx.Done():
		t.Errorf("timed out waiting for value: %d", expect)
	}
}
