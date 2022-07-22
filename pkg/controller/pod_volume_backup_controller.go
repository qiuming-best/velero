/*
Copyright The Velero Contributors.

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

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

// PodVolumeBackupReconciler reconciles a PodVolumeBackup object
type PodVolumeBackupReconciler struct {
	Scheme         *runtime.Scheme
	Client         client.Client
	Clock          clock.Clock
	Metrics        *metrics.ServerMetrics
	CredsFileStore credentials.FileStore
	NodeName       string
	FileSystem     filesystem.Interface
	Log            logrus.FieldLogger
	Ctx            context.Context
	LegacyUploader bool
}

// +kubebuilder:rbac:groups=velero.io,resources=podvolumebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=podvolumebackups/status,verbs=get;update;patch
func (r *PodVolumeBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithFields(logrus.Fields{
		"controller":      "podvolumebackup",
		"podvolumebackup": req.NamespacedName,
	})

	var pvb velerov1api.PodVolumeBackup
	if err := r.Client.Get(ctx, req.NamespacedName, &pvb); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find PodVolumeBackup")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "getting PodVolumeBackup")
	}

	if len(pvb.OwnerReferences) == 1 {
		log = log.WithField(
			"backup",
			fmt.Sprintf("%s/%s", req.Namespace, pvb.OwnerReferences[0].Name),
		)
	}

	log.Info("PodVolumeBackup starting")

	// Only process items for this node.
	if pvb.Spec.Node != r.NodeName {
		return ctrl.Result{}, nil
	}

	switch pvb.Status.Phase {
	case "", velerov1api.PodVolumeBackupPhaseNew:
	case velerov1api.PodVolumeBackupPhaseInProgress:
		original := pvb.DeepCopy()
		pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseFailed
		pvb.Status.Message = fmt.Sprintf("got a PodVolumeBackup with unexpected status %q, this may be due to a restart of the controller during the backing up, mark it as %q",
			velerov1api.PodVolumeBackupPhaseInProgress, pvb.Status.Phase)
		pvb.Status.CompletionTimestamp = &metav1.Time{Time: r.Clock.Now()}
		if err := kube.Patch(ctx, original, &pvb, r.Client); err != nil {
			log.WithError(err).Error("error updating PodVolumeBackup status")
			return ctrl.Result{}, err
		}
		log.Warn(pvb.Status.Message)
		return ctrl.Result{}, nil
	default:
		log.Debug("PodVolumeBackup is not new or in-progress, not processing")
		return ctrl.Result{}, nil
	}

	r.Metrics.RegisterPodVolumeBackupEnqueue(r.NodeName)

	// Update status to InProgress.
	original := pvb.DeepCopy()
	pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseInProgress
	pvb.Status.StartTimestamp = &metav1.Time{Time: r.Clock.Now()}
	if err := kube.Patch(ctx, original, &pvb, r.Client); err != nil {
		log.WithError(err).Error("error updating PodVolumeBackup status")
		return ctrl.Result{}, err
	}

	var pod corev1.Pod
	podNamespacedName := client.ObjectKey{
		Namespace: pvb.Spec.Pod.Namespace,
		Name:      pvb.Spec.Pod.Name,
	}
	if err := r.Client.Get(ctx, podNamespacedName, &pod); err != nil {
		return r.updateStatusToFailed(ctx, &pvb, err, fmt.Sprintf("getting pod %s/%s", pvb.Spec.Pod.Namespace, pvb.Spec.Pod.Name), log)
	}

	volDir, err := kube.GetVolumeDirectory(ctx, log, &pod, pvb.Spec.Volume, r.Client)
	if err != nil {
		return r.updateStatusToFailed(ctx, &pvb, err, "getting volume directory name", log)
	}

	pathGlob := fmt.Sprintf("/host_pods/%s/volumes/*/%s", string(pvb.Spec.Pod.UID), volDir)
	log.WithField("pathGlob", pathGlob).Debug("Looking for path matching glob")

	path, err := r.singlePathMatch(pathGlob)
	if err != nil {
		return r.updateStatusToFailed(ctx, &pvb, err, "identifying unique volume path on host", log)
	}
	log.WithField("path", path).Debugf("Found path matching glob")

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{
		Namespace: pvb.Namespace,
		Name:      pvb.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		return r.updateStatusToFailed(ctx, &pvb, err, "error getting backup storage location", log)
	}

	var uploaderProv uploader.UploaderProvider
	uploaderProv, err = uploader.NewUploaderProvider(
		r.Ctx, r.LegacyUploader, pvb.Spec.RepoIdentifier, pvb.Namespace, backupLocation,
		r.CredsFileStore, repository.RepoKeySelector(), r.Client, "", log, "backup")
	if err != nil {
		return r.updateStatusToFailed(ctx, &pvb, err, "error creating uploader", log)
	}

	// If this is a PVC, look for the most recent completed pod volume backup for it and get
	// its restic snapshot ID to use as the value of the `--parent` flag. Without this,
	// if the pod using the PVC (and therefore the directory path under /host_pods/) has
	// changed since the PVC's last backup, restic will not be able to identify a suitable
	// parent snapshot to use, and will have to do a full rescan of the contents of the PVC.
	var parentSnapshotID string
	if pvcUID, ok := pvb.Labels[velerov1api.PVCUIDLabel]; ok {
		parentSnapshotID = r.getParentSnapshot(ctx, log, pvb.Namespace, pvcUID, pvb.Spec.BackupStorageLocation)
		if parentSnapshotID == "" {
			log.Info("No parent snapshot found for PVC, not using --parent flag for this backup")
		} else {
			log.WithField("parentSnapshotID", parentSnapshotID).
				Info("Setting --parent flag for this backup")
		}
	}

	var stdout, stderr string

	var emptySnapshot bool
	if stdout, stderr, err = uploaderProv.RunBackup(ctx, path, pvb.Spec.Tags, parentSnapshotID, r.updateBackupProgressFunc(&pvb, log)); err != nil {
		if strings.Contains(stderr, "snapshot is empty") {
			emptySnapshot = true
		} else {
			log.WithError(errors.WithStack(err)).Errorf("Error running uploader backup=%s, stdout=%s, stderr=%s", uploaderProv.GetTaskName(), stdout, stderr)
			return r.updateStatusToFailed(ctx, &pvb, err, "error running uploader backup", log)
		}
	}
	log.Debugf("Ran command=%s, stdout=%s, stderr=%s", uploaderProv.GetTaskName(), stdout, stderr)

	var snapshotID string
	if !emptySnapshot {
		snapshotID, err = uploaderProv.GetSnapshotID()
		if err != nil {
			return r.updateStatusToFailed(ctx, &pvb, err, "getting snapshot id", log)
		}
	}

	// Update status to Completed with path & snapshot ID.
	original = pvb.DeepCopy()
	pvb.Status.Path = path
	pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseCompleted
	pvb.Status.SnapshotID = snapshotID
	pvb.Status.CompletionTimestamp = &metav1.Time{Time: r.Clock.Now()}
	if emptySnapshot {
		pvb.Status.Message = "volume was empty so no snapshot was taken"
	}
	if err = kube.Patch(ctx, original, &pvb, r.Client); err != nil {
		log.WithError(err).Error("error updating PodVolumeBackup status")
		return ctrl.Result{}, err
	}

	latencyDuration := pvb.Status.CompletionTimestamp.Time.Sub(pvb.Status.StartTimestamp.Time)
	latencySeconds := float64(latencyDuration / time.Second)
	backupName := fmt.Sprintf("%s/%s", req.Namespace, pvb.OwnerReferences[0].Name)
	r.Metrics.ObserveResticOpLatency(r.NodeName, req.Name, uploaderProv.GetTaskName(), backupName, latencySeconds)
	r.Metrics.RegisterResticOpLatencyGauge(r.NodeName, req.Name, uploaderProv.GetTaskName(), backupName, latencySeconds)
	r.Metrics.RegisterPodVolumeBackupDequeue(r.NodeName)

	log.Info("PodVolumeBackup completed with status %v", pvb.Status.Phase)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the PVB controller.
func (r *PodVolumeBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.PodVolumeBackup{}).
		Complete(r)
}

func (r *PodVolumeBackupReconciler) singlePathMatch(path string) (string, error) {
	matches, err := r.FileSystem.Glob(path)
	if err != nil {
		return "", errors.WithStack(err)
	}

	if len(matches) != 1 {
		return "", errors.Errorf("expected one matching path, got %d", len(matches))
	}

	return matches[0], nil
}

// getParentSnapshot finds the most recent completed PodVolumeBackup for the
// specified PVC and returns its Restic snapshot ID. Any errors encountered are
// logged but not returned since they do not prevent a backup from proceeding.
func (r *PodVolumeBackupReconciler) getParentSnapshot(ctx context.Context, log logrus.FieldLogger, pvbNamespace, pvcUID, bsl string) string {
	log = log.WithField("pvcUID", pvcUID)
	log.Infof("Looking for most recent completed PodVolumeBackup for this PVC")

	listOpts := &client.ListOptions{
		Namespace: pvbNamespace,
	}
	matchingLabels := client.MatchingLabels(map[string]string{velerov1api.PVCUIDLabel: pvcUID})
	matchingLabels.ApplyToList(listOpts)

	var pvbList velerov1api.PodVolumeBackupList
	if err := r.Client.List(ctx, &pvbList, listOpts); err != nil {
		log.WithError(errors.WithStack(err)).Error("getting list of podvolumebackups for this PVC")
	}

	// Go through all the podvolumebackups for the PVC and look for the most
	// recent completed one to use as the parent.
	var mostRecentPVB *velerov1api.PodVolumeBackup
	for _, pvb := range pvbList.Items {
		if pvb.Status.Phase != velerov1api.PodVolumeBackupPhaseCompleted {
			continue
		}

		if bsl != pvb.Spec.BackupStorageLocation {
			// Check the backup storage location is the same as spec in order to
			// support backup to multiple backup-locations. Otherwise, there exists
			// a case that backup volume snapshot to the second location would
			// failed, since the founded parent ID is only valid for the first
			// backup location, not the second backup location. Also, the second
			// backup should not use the first backup parent ID since its for the
			// first backup location only.
			continue
		}

		if mostRecentPVB == nil || pvb.Status.StartTimestamp.After(mostRecentPVB.Status.StartTimestamp.Time) {
			mostRecentPVB = &pvb
		}
	}

	if mostRecentPVB == nil {
		log.Info("No completed PodVolumeBackup found for PVC")
		return ""
	}

	log.WithFields(map[string]interface{}{
		"parentPodVolumeBackup": mostRecentPVB.Name,
		"parentSnapshotID":      mostRecentPVB.Status.SnapshotID,
	}).Info("Found most recent completed PodVolumeBackup for PVC")

	return mostRecentPVB.Status.SnapshotID
}

// updateBackupProgressFunc returns a func that takes progress info and patches
// the PVB with the new progress.
func (r *PodVolumeBackupReconciler) updateBackupProgressFunc(pvb *velerov1api.PodVolumeBackup, log logrus.FieldLogger) func(pro velerov1api.PodVolumeOperationProgress, msg string) {
	return func(progress velerov1api.PodVolumeOperationProgress, mgs string) {
		original := pvb.DeepCopy()
		pvb.Status.Progress = progress
		if err := kube.Patch(context.Background(), original, pvb, r.Client); err != nil {
			log.WithError(err).Error("error update progress")
		}
		log.Infof("vae progress %v %s", progress, mgs)
	}
}

func (r *PodVolumeBackupReconciler) updateStatusToFailed(ctx context.Context, pvb *velerov1api.PodVolumeBackup, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	original := pvb.DeepCopy()
	pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseFailed
	pvb.Status.Message = msg
	pvb.Status.CompletionTimestamp = &metav1.Time{Time: r.Clock.Now()}

	if err = kube.Patch(ctx, original, pvb, r.Client); err != nil {
		log.WithError(err).Error("error updating PodVolumeBackup status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}
