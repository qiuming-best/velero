/*
Copyright 2018 the Velero contributors.

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

package podvolume

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/resourcepolicies"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	veleroclient "github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/repository"
	uploaderutil "github.com/vmware-tanzu/velero/pkg/uploader/util"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

// Backupper can execute pod volume backups of volumes in a pod.
type Backupper interface {
	// BackupPodVolumes backs up all specified volumes in a pod.
	BackupPodVolumes(backup *velerov1api.Backup, pod *corev1api.Pod, volumesToBackup []string, resPolicies *resourcepolicies.Policies, log logrus.FieldLogger) ([]*velerov1api.PodVolumeBackup, *PVCBackupSummary, []error)
}

type backupper struct {
	ctx          context.Context
	repoLocker   *repository.RepoLocker
	repoEnsurer  *repository.Ensurer
	crClient     ctrlclient.Client
	uploaderType string

	results     map[string]chan *velerov1api.PodVolumeBackup
	resultsLock sync.Mutex
}

type skippedPVC struct {
	PVC    *corev1api.PersistentVolumeClaim
	Reason string
}

// PVCBackupSummary is a summary for which PVCs are skipped, which are backed up after each execution of the Backupper
// The scope should be within one pod, so the volume name is the key for the maps
type PVCBackupSummary struct {
	Backedup map[string]*corev1api.PersistentVolumeClaim
	Skipped  map[string]*skippedPVC
	pvcMap   map[string]*corev1api.PersistentVolumeClaim
}

func NewPVCBackupSummary() *PVCBackupSummary {
	return &PVCBackupSummary{
		Backedup: make(map[string]*corev1api.PersistentVolumeClaim),
		Skipped:  make(map[string]*skippedPVC),
		pvcMap:   make(map[string]*corev1api.PersistentVolumeClaim),
	}
}

func (pbs *PVCBackupSummary) addBackedup(volumeName string) {
	if pvc, ok := pbs.pvcMap[volumeName]; ok {
		pbs.Backedup[volumeName] = pvc
		delete(pbs.Skipped, volumeName)
	}
}

func (pbs *PVCBackupSummary) addSkipped(volumeName string, reason string) {
	if pvc, ok := pbs.pvcMap[volumeName]; ok {
		if _, ok2 := pbs.Backedup[volumeName]; !ok2 { // if it's not backed up, add it to skipped
			pbs.Skipped[volumeName] = &skippedPVC{
				PVC:    pvc,
				Reason: reason,
			}
		}
	}
}

func (pbs *PVCBackupSummary) isSkipped(volumeName string) bool {
	_, ok := pbs.Skipped[volumeName]
	return ok
}

func newBackupper(
	ctx context.Context,
	repoLocker *repository.RepoLocker,
	repoEnsurer *repository.Ensurer,
	pvbInformer ctrlcache.Informer,
	crClient ctrlclient.Client,
	uploaderType string,
	backup *velerov1api.Backup,
	log logrus.FieldLogger,
) *backupper {
	b := &backupper{
		ctx:          ctx,
		repoLocker:   repoLocker,
		repoEnsurer:  repoEnsurer,
		crClient:     crClient,
		uploaderType: uploaderType,

		results: make(map[string]chan *velerov1api.PodVolumeBackup),
	}

	pvbInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pvb := obj.(*velerov1api.PodVolumeBackup)

				if pvb.GetLabels()[velerov1api.BackupUIDLabel] != string(backup.UID) {
					return
				}

				if pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseCompleted || pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseFailed {
					b.resultsLock.Lock()
					defer b.resultsLock.Unlock()

					resChan, ok := b.results[resultsKey(pvb.Spec.Pod.Namespace, pvb.Spec.Pod.Name)]
					if !ok {
						log.Errorf("No results channel found for pod %s/%s to send pod volume backup %s/%s on", pvb.Spec.Pod.Namespace, pvb.Spec.Pod.Name, pvb.Namespace, pvb.Name)
						return
					}
					resChan <- pvb
				}
			},
		},
	)

	return b
}

func resultsKey(ns, name string) string {
	return fmt.Sprintf("%s/%s", ns, name)
}

func (b *backupper) getMatchAction(resPolicies *resourcepolicies.Policies, pvc *corev1api.PersistentVolumeClaim, volume *corev1api.Volume) (*resourcepolicies.Action, error) {
	if pvc != nil {
		pv := new(corev1api.PersistentVolume)
		err := b.crClient.Get(context.TODO(), ctrlclient.ObjectKey{Name: pvc.Spec.VolumeName}, pv)
		if err != nil {
			return nil, errors.Wrapf(err, "error getting pv for pvc %s", pvc.Spec.VolumeName)
		}
		return resPolicies.GetMatchAction(pv)
	}

	if volume != nil {
		return resPolicies.GetMatchAction(volume)
	}

	return nil, errors.Errorf("failed to check resource policies for empty volume")
}

func (b *backupper) BackupPodVolumes(backup *velerov1api.Backup, pod *corev1api.Pod, volumesToBackup []string, resPolicies *resourcepolicies.Policies, log logrus.FieldLogger) ([]*velerov1api.PodVolumeBackup, *PVCBackupSummary, []error) {
	if len(volumesToBackup) == 0 {
		return nil, nil, nil
	}
	log.Infof("pod %s/%s has volumes to backup: %v", pod.Namespace, pod.Name, volumesToBackup)

	var (
		pvcSummary = NewPVCBackupSummary()
		podVolumes = make(map[string]corev1api.Volume)
		errs       = []error{}
	)

	// put the pod's volumes and the PVC associated in maps for efficient lookup below
	for _, podVolume := range pod.Spec.Volumes {
		podVolumes[podVolume.Name] = podVolume
		if podVolume.PersistentVolumeClaim != nil {
			pvc := new(corev1api.PersistentVolumeClaim)
			err := b.crClient.Get(context.TODO(), ctrlclient.ObjectKey{Namespace: pod.Namespace, Name: podVolume.PersistentVolumeClaim.ClaimName}, pvc)
			if err != nil {
				errs = append(errs, errors.Wrap(err, "error getting persistent volume claim for volume"))
				continue
			}
			pvcSummary.pvcMap[podVolume.Name] = pvc
		}
	}

	hasVolumeNeedBackup := false
	if resPolicies != nil {
		for _, volumeName := range volumesToBackup {
			pvc, ok := pvcSummary.pvcMap[volumeName]
			if !ok {
				// there should have been error happened retrieving the PVC and it's recorded already
				continue
			}
			if action, err := b.getMatchAction(resPolicies, pvc, nil); err != nil {
				errs = append(errs, errors.Wrapf(err, "error getting policy action for pvc %s", pvc.Spec.VolumeName))
				continue
			} else if action != nil {
				if action.Type != resourcepolicies.Skip {
					hasVolumeNeedBackup = true
				} else {
					pvcSummary.addSkipped(volumeName, "matched action is 'skip' in chosen resource policies")
				}
			}

			podVolume, ok := podVolumes[volumeName]
			if !ok {
				log.Warnf("No volume named %s found in pod %s/%s, skipping", volumeName, pod.Namespace, pod.Name)
				continue
			}
			if action, err := b.getMatchAction(resPolicies, nil, &podVolume); err != nil {
				errs = append(errs, errors.Wrapf(err, "error getting policy action for volume %s", volumeName))
				continue
			} else if action != nil {
				if action.Type != resourcepolicies.Skip {
					hasVolumeNeedBackup = true
				} else {
					pvcSummary.addSkipped(volumeName, "matched action is 'skip' in chosen resource policies")
				}
			}
		}
	}

	if !hasVolumeNeedBackup {
		log.Infof("skip backup of pod %s/%s because no volume need backup", pod.Namespace, pod.Name)
		return nil, pvcSummary, nil
	}

	if err := kube.IsPodRunning(pod); err != nil {
		skipAllPodVolumes(pod, volumesToBackup, err, pvcSummary, log)
		return nil, pvcSummary, nil
	}

	err := nodeagent.IsRunningInNode(b.ctx, backup.Namespace, pod.Spec.NodeName, b.crClient)
	if err != nil {
		return nil, nil, []error{err}
	}

	repositoryType := getRepositoryType(b.uploaderType)
	if repositoryType == "" {
		err := errors.Errorf("empty repository type, uploader %s", b.uploaderType)
		return nil, nil, []error{err}
	}

	repo, err := b.repoEnsurer.EnsureRepo(b.ctx, backup.Namespace, pod.Namespace, backup.Spec.StorageLocation, repositoryType)
	if err != nil {
		return nil, nil, []error{err}
	}

	// get a single non-exclusive lock since we'll wait for all individual
	// backups to be complete before releasing it.
	b.repoLocker.Lock(repo.Name)
	defer b.repoLocker.Unlock(repo.Name)

	resultsChan := make(chan *velerov1api.PodVolumeBackup)

	b.resultsLock.Lock()
	b.results[resultsKey(pod.Namespace, pod.Name)] = resultsChan
	b.resultsLock.Unlock()

	var (
		podVolumeBackups   []*velerov1api.PodVolumeBackup
		mountedPodVolumes  = sets.String{}
		attachedPodDevices = sets.String{}
	)

	for _, container := range pod.Spec.Containers {
		for _, volumeMount := range container.VolumeMounts {
			mountedPodVolumes.Insert(volumeMount.Name)
		}
		for _, volumeDevice := range container.VolumeDevices {
			attachedPodDevices.Insert(volumeDevice.Name)
		}
	}

	repoIdentifier := ""
	if repositoryType == velerov1api.BackupRepositoryTypeRestic {
		repoIdentifier = repo.Spec.ResticIdentifier
	}

	var numVolumeSnapshots int
	for _, volumeName := range volumesToBackup {
		if pvcSummary.isSkipped(volumeName) {
			continue
		}

		volume, ok := podVolumes[volumeName]
		if !ok {
			// already logged in the previous loop
			continue
		}
		var pvc *corev1api.PersistentVolumeClaim
		if volume.PersistentVolumeClaim != nil {
			pvc, ok = pvcSummary.pvcMap[volumeName]
			if !ok {
				// there should have been error happened retrieving the PVC and it's recorded already
				continue
			}
		}

		// hostPath volumes are not supported because they're not mounted into /var/lib/kubelet/pods, so our
		// daemonset pod has no way to access their data.
		isHostPath, err := isHostPathVolume(&volume, pvc, b.crClient)
		if err != nil {
			errs = append(errs, errors.Wrap(err, "error checking if volume is a hostPath volume"))
			continue
		}
		if isHostPath {
			log.Warnf("Volume %s in pod %s/%s is a hostPath volume which is not supported for pod volume backup, skipping", volumeName, pod.Namespace, pod.Name)
			continue
		}

		// check if volume is a block volume
		if attachedPodDevices.Has(volumeName) {
			msg := fmt.Sprintf("volume %s declared in pod %s/%s is a block volume. Block volumes are not supported for fs backup, skipping",
				volumeName, pod.Namespace, pod.Name)
			log.Warn(msg)
			pvcSummary.addSkipped(volumeName, msg)
			continue
		}

		// volumes that are not mounted by any container should not be backed up, because
		// its directory is not created
		if !mountedPodVolumes.Has(volumeName) {
			msg := fmt.Sprintf("volume %s is declared in pod %s/%s but not mounted by any container, skipping", volumeName, pod.Namespace, pod.Name)
			log.Warn(msg)
			pvcSummary.addSkipped(volumeName, msg)
			continue
		}

		volumeBackup := newPodVolumeBackup(backup, pod, volume, repoIdentifier, b.uploaderType, pvc)
		if err := veleroclient.CreateRetryGenerateName(b.crClient, b.ctx, volumeBackup); err != nil {
			errs = append(errs, err)
			continue
		}
		pvcSummary.addBackedup(volumeName)
		numVolumeSnapshots++
	}

ForEachVolume:
	for i, count := 0, numVolumeSnapshots; i < count; i++ {
		select {
		case <-b.ctx.Done():
			errs = append(errs, errors.New("timed out waiting for all PodVolumeBackups to complete"))
			break ForEachVolume
		case res := <-resultsChan:
			switch res.Status.Phase {
			case velerov1api.PodVolumeBackupPhaseCompleted:
				podVolumeBackups = append(podVolumeBackups, res)
			case velerov1api.PodVolumeBackupPhaseFailed:
				errs = append(errs, errors.Errorf("pod volume backup failed: %s", res.Status.Message))
				podVolumeBackups = append(podVolumeBackups, res)
			}
		}
	}

	b.resultsLock.Lock()
	delete(b.results, resultsKey(pod.Namespace, pod.Name))
	b.resultsLock.Unlock()

	return podVolumeBackups, pvcSummary, errs
}

func skipAllPodVolumes(pod *corev1api.Pod, volumesToBackup []string, err error, pvcSummary *PVCBackupSummary, log logrus.FieldLogger) {
	for _, volumeName := range volumesToBackup {
		log.WithError(err).Warnf("Skip pod volume %s", volumeName)
		pvcSummary.addSkipped(volumeName, fmt.Sprintf("encountered a problem with backing up the PVC of pod %s/%s: %v", pod.Namespace, pod.Name, err))
	}
}

// isHostPathVolume returns true if the volume is either a hostPath pod volume or a persistent
// volume claim on a hostPath persistent volume, or false otherwise.
func isHostPathVolume(volume *corev1api.Volume, pvc *corev1api.PersistentVolumeClaim, crClient ctrlclient.Client) (bool, error) {
	if volume.HostPath != nil {
		return true, nil
	}

	if pvc == nil || pvc.Spec.VolumeName == "" {
		return false, nil
	}

	pv := new(corev1api.PersistentVolume)
	err := crClient.Get(context.TODO(), ctrlclient.ObjectKey{Name: pvc.Spec.VolumeName}, pv)
	if err != nil {
		return false, errors.WithStack(err)
	}

	return pv.Spec.HostPath != nil, nil
}

func newPodVolumeBackup(backup *velerov1api.Backup, pod *corev1api.Pod, volume corev1api.Volume, repoIdentifier, uploaderType string, pvc *corev1api.PersistentVolumeClaim) *velerov1api.PodVolumeBackup {
	pvb := &velerov1api.PodVolumeBackup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    backup.Namespace,
			GenerateName: backup.Name + "-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "Backup",
					Name:       backup.Name,
					UID:        backup.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.BackupNameLabel: label.GetValidName(backup.Name),
				velerov1api.BackupUIDLabel:  string(backup.UID),
			},
		},
		Spec: velerov1api.PodVolumeBackupSpec{
			Node: pod.Spec.NodeName,
			Pod: corev1api.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       pod.UID,
			},
			Volume: volume.Name,
			Tags: map[string]string{
				"backup":     backup.Name,
				"backup-uid": string(backup.UID),
				"pod":        pod.Name,
				"pod-uid":    string(pod.UID),
				"ns":         pod.Namespace,
				"volume":     volume.Name,
			},
			BackupStorageLocation: backup.Spec.StorageLocation,
			RepoIdentifier:        repoIdentifier,
			UploaderType:          uploaderType,
		},
	}

	if pvc != nil {
		// this annotation is used in pkg/restore to identify if a PVC
		// has a pod volume backup.
		pvb.Annotations = map[string]string{
			PVCNameAnnotation: pvc.Name,
		}

		// this label is used by the pod volume backup controller to tell
		// if a pod volume backup is for a PVC.
		pvb.Labels[velerov1api.PVCUIDLabel] = string(pvc.UID)

		// this tag is not used by velero, but useful for debugging.
		pvb.Spec.Tags["pvc-uid"] = string(pvc.UID)
	}

	if backup.Spec.UploaderConfig != nil {
		pvb.Spec.UploaderSettings = uploaderutil.StoreBackupConfig(backup.Spec.UploaderConfig)
	}

	return pvb
}
