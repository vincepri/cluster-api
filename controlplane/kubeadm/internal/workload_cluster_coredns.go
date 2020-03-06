/*
Copyright 2020 The Kubernetes Authors.

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

package internal

import (
	"context"
	"fmt"

	"github.com/blang/semver"
	"github.com/coredns/corefile-migration/migration"
	"github.com/docker/distribution/reference"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeadmv1 "sigs.k8s.io/cluster-api/bootstrap/kubeadm/types/v1beta1"
	controlplanev1 "sigs.k8s.io/cluster-api/controlplane/kubeadm/api/v1alpha3"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	corefileKey       = "Corefile"
	corefileBackupKey = "Corefile-backup"
	coreDNSKey        = "coredns"
)

type coreDNSMigrator interface {
	Migrate(currentVersion string, toVersion string, corefile string, deprecations bool) (string, error)
}

type CoreDNSMigrator struct{}

func (c *CoreDNSMigrator) Migrate(fromCoreDNSVersion, toCoreDNSVersion, corefile string, deprecations bool) (string, error) {
	return migration.Migrate(fromCoreDNSVersion, toCoreDNSVersion, corefile, deprecations)
}

type coreDNSInfo struct {
	Corefile       string
	Deployment     *appsv1.Deployment
	CurrentVersion string
}

// UpdateCoreDNS updates the kubeadm configmap, coredns corefile and coredns
// deployment.
func (w *Workload) UpdateCoreDNS(ctx context.Context, kcp *controlplanev1.KubeadmControlPlane) error {
	info, err := w.getCoreDNSInfo(ctx)
	if err != nil {
		return err
	}
	if kcp.Spec.KubeadmConfigSpec.ClusterConfiguration == nil {
		return nil
	}

	// Validate the image tag.
	if err := validateCoreDNSImageTag(info.CurrentVersion, kcp.Spec.KubeadmConfigSpec.ClusterConfiguration.DNS.ImageTag); err != nil {
		return errors.Wrapf(err, "failed to validate CoreDNS")
	}

	// Get the DNS cluster configuration.
	dns := &kcp.Spec.KubeadmConfigSpec.ClusterConfiguration.DNS

	// Perform the upgrade.
	if err := w.updateCoreDNSImageInfoInKubeadmConfigMap(ctx, dns); err != nil {
		return err
	}
	if err := w.updateCoreDNSCorefile(ctx, info, dns); err != nil {
		return err
	}
	if err := w.updateCoreDNSDeployment(ctx, info, dns); err != nil {
		return errors.Wrap(err, "unable to update coredns deployment")
	}
	return nil
}

// validateCoreDNSImageTag returns error if the versions don't meet requirements.
// Some of the checks come from
// https://github.com/coredns/corefile-migration/blob/v1.0.6/migration/migrate.go#L414
func validateCoreDNSImageTag(fromVersion, toVersion string) error {
	from, err := semver.Parse(fromVersion)
	if err != nil {
		return errors.Wrapf(err, "failed to semver parse %q", fromVersion)
	}
	to, err := semver.Parse(toVersion)
	if err != nil {
		return errors.Wrapf(err, "failed to semver parse %q", toVersion)
	}
	if from.Compare(to) >= 0 {
		return fmt.Errorf("toVersion %q must be greater than fromVersion %q", to.String(), from.String())
	}

	// check if the from version is even in the list of coredns versions
	if _, ok := migration.Versions[fmt.Sprintf("%d.%d.%d", from.Major, from.Minor, from.Patch)]; !ok {
		return fmt.Errorf("fromVersion %q is not a compatible coredns version", from.String())
	}
	return nil
}

// getCoreDNSInfo gets
// - the current CoreDNS image version installed
// - the current Corefile Configuration of CoreDNS
// - the coredns deployment
func (w *Workload) getCoreDNSInfo(ctx context.Context) (*coreDNSInfo, error) {
	// Get the coredns configmap and corefile.
	corednsKey := ctrlclient.ObjectKey{Name: coreDNSKey, Namespace: metav1.NamespaceSystem}
	cm := &corev1.ConfigMap{}
	if err := w.Client.Get(ctx, corednsKey, cm); err != nil {
		return nil, errors.Wrapf(err, "error getting %s config map from target cluster", corednsKey.String())
	}
	corefile, ok := cm.Data[corefileKey]
	if !ok {
		return nil, errors.New("unable to find the CoreDNS Corefile data")
	}

	// Get the current CoreDNS deployment.
	deployment := &appsv1.Deployment{}
	if err := w.Client.Get(ctx, corednsKey, deployment); err != nil {
		return nil, errors.Wrapf(err, "unable to get %s deployment from target cluster", corednsKey.String())
	}

	// Parse deployment image.
	addonImage := deployment.Spec.Template.Spec.Containers[0].Image
	ref, err := reference.ParseNormalizedNamed(addonImage)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse %s deployment image", corednsKey.String())
	}
	tagged, hasTag := ref.(reference.Tagged)
	if !hasTag {
		return nil, errors.Errorf("%s deployment does not have a valid image tag", corednsKey.String())
	}
	parsedTag, err := parseImageTag(tagged.Tag())
	if err != nil {
		return nil, err
	}

	return &coreDNSInfo{
		Corefile:       corefile,
		Deployment:     deployment,
		CurrentVersion: parsedTag,
	}, nil
}

// UpdateCoreDNSDeployment will patch the deployment image to the
// imageRepo:imageTag in the KCP dns. It will also ensure the volume of the
// deployment uses the Corefile key of the coredns configmap.
func (w *Workload) updateCoreDNSDeployment(ctx context.Context, info *coreDNSInfo, dns *kubeadmv1.DNS) error {
	containers := info.Deployment.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return errors.New("failed to update coredns deployment: deployment spec has no containers")
	}

	// Parse deployment image.
	image := info.Deployment.Spec.Template.Spec.Containers[0].Image
	imageRef, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return errors.Wrapf(err, "unable to parse %q deployment image", image)
	}

	// Handle imageRepository.
	imageRepository := fmt.Sprintf("%s/%s", reference.Domain(imageRef), reference.Path(imageRef))
	if dns.ImageRepository != "" {
		imageRepository = dns.ImageRepository
	}

	// Handle imageTag.
	imageRefTag, ok := imageRef.(reference.Tagged)
	if !ok {
		return errors.Errorf("failed to update coredns deployment: does not have a valid image tag: %q", image)
	}
	imageTag := imageRefTag.Tag()
	if dns.ImageTag != "" {
		imageTag = dns.ImageTag
	}

	// Create a patch helper.
	helper, err := patch.NewHelper(info.Deployment, w.Client)
	if err != nil {
		return err
	}

	// Form the final image before issuing the patch.
	deploymentImage := fmt.Sprintf("%s:%s", imageRepository, imageTag)
	patchCoreDNSDeploymentImage(info.Deployment, deploymentImage)
	patchCoreDNSDeploymentVolume(info.Deployment, corefileBackupKey, corefileKey)
	return helper.Patch(ctx, info.Deployment)
}

// UpdateCoreDNSImageInfoInKubeadmConfigMap updates the kubernetes version in the kubeadm config map.
func (w *Workload) updateCoreDNSImageInfoInKubeadmConfigMap(ctx context.Context, dns *kubeadmv1.DNS) error {
	if dns.Type != "" && dns.Type != kubeadmv1.CoreDNS {
		// do nothing if it is not CoreDNS
		return nil
	}
	configMapKey := ctrlclient.ObjectKey{Name: "kubeadm-config", Namespace: metav1.NamespaceSystem}
	kubeadmConfigMap, err := w.getConfigMap(ctx, configMapKey)
	if err != nil {
		return err
	}
	config := &kubeadmConfig{ConfigMap: kubeadmConfigMap}
	if err := config.UpdateCoreDNSImageInfo(dns.ImageRepository, dns.ImageTag); err != nil {
		return err
	}
	if err := w.Client.Update(ctx, config.ConfigMap); err != nil {
		return errors.Wrap(err, "error updating kubeadm ConfigMap")
	}
	return nil
}

// updateCoreDNSCorefile migrates the coredns corefile if there is an increase
// in version number. It also creates a corefile backup and patches the
// deployment to point to the backup corefile before migrating.
func (w *Workload) updateCoreDNSCorefile(ctx context.Context, info *coreDNSInfo, dns *kubeadmv1.DNS) error {
	if dns.Type != "" && dns.Type != kubeadmv1.CoreDNS {
		// do nothing if it is not CoreDNS
		return nil
	}

	// Parse and validate the coredns image tag.
	toCoreDNSVersion, err := parseImageTag(dns.ImageTag)
	if err != nil {
		return err
	}

	// First we backup the Corefile by backing it up.
	if err := w.Client.Update(ctx, &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "coredns",
			Namespace: metav1.NamespaceSystem,
		},
		Data: map[string]string{
			corefileKey:       info.Corefile,
			corefileBackupKey: info.Corefile,
		},
	}); err != nil {
		return errors.Wrap(err, "unable to update CoreDNS config map with backup Corefile")
	}

	// Patching the coredns deployment to point to the Corefile-backup
	// contents before performing the migration.
	helper, err := patch.NewHelper(info.Deployment, w.Client)
	if err != nil {
		return err
	}
	patchCoreDNSDeploymentVolume(info.Deployment, corefileKey, corefileBackupKey)
	if err := helper.Patch(ctx, info.Deployment); err != nil {
		return err
	}

	// Run the CoreDNS migration tool.
	updatedCorefile, err := w.CoreDNSMigrator.Migrate(info.CurrentVersion, toCoreDNSVersion, info.Corefile, false)
	if err != nil {
		return errors.Wrap(err, "unable to migrate CoreDNS corefile")
	}
	if err := w.Client.Update(ctx, &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "coredns",
			Namespace: metav1.NamespaceSystem,
		},
		Data: map[string]string{
			corefileKey:       updatedCorefile,
			corefileBackupKey: info.Corefile,
		},
	}); err != nil {
		return errors.Wrap(err, "unable to update CoreDNS config map")
	}

	return nil
}

func patchCoreDNSDeploymentVolume(deployment *appsv1.Deployment, fromKey, toKey string) {
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == "config-volume" && volume.ConfigMap != nil && volume.ConfigMap.Name == "coredns" {
			for i, item := range volume.ConfigMap.Items {
				if item.Key == fromKey || item.Key == toKey {
					volume.ConfigMap.Items[i].Key = toKey
				}
			}
		}
	}
}

func patchCoreDNSDeploymentImage(deployment *appsv1.Deployment, image string) {
	containers := deployment.Spec.Template.Spec.Containers
	for idx, c := range containers {
		if c.Name == "coredns" {
			containers[idx].Image = image
		}
	}
}

func parseImageTag(tag string) (string, error) {
	ver, err := semver.Parse(tag)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d.%d.%d", ver.Major, ver.Minor, ver.Patch), nil
}
