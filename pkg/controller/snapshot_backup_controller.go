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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/repository"
	repokey "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/provider"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

const (
	PollInterval = 2 * time.Second
	PollTimeout  = 5 * time.Minute
)

// SnapshotBackupReconciler reconciles a Snapshotbackup object
type SnapshotBackupReconciler struct {
	Scheme            *runtime.Scheme
	Client            client.Client
	kubeClient        kubernetes.Interface
	Clock             clock.Clock
	Metrics           *metrics.ServerMetrics
	CredentialGetter  *credentials.CredentialGetter
	NodeName          string
	FileSystem        filesystem.Interface
	Log               logrus.FieldLogger
	RepositoryEnsurer *repository.RepositoryEnsurer
}

type SnapshotBackupProgressUpdater struct {
	SnapshotBackup *velerov1api.SnapshotBackup
	Log            logrus.FieldLogger
	Ctx            context.Context
	Cli            client.Client
}

func NewSnapshotBackupReconciler(scheme *runtime.Scheme, client client.Client, kubeClient kubernetes.Interface, clock clock.Clock, metrics *metrics.ServerMetrics, cred *credentials.CredentialGetter, nodeName string, fs filesystem.Interface, log logrus.FieldLogger) *SnapshotBackupReconciler {
	return &SnapshotBackupReconciler{scheme, client, kubeClient, clock, metrics, cred, nodeName, fs, log, repository.NewRepositoryEnsurer(client, log)}
}

// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackup,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackup/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumerclaims,verbs=get
func (s *SnapshotBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := s.Log.WithFields(logrus.Fields{
		"controller":     "snapshotbackup",
		"snapshotbackup": req.NamespacedName,
	})
	var ssb velerov1api.SnapshotBackup
	if err := s.Client.Get(ctx, req.NamespacedName, &ssb); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find SnapshotBackup")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "getting SnapshotBackup")
	}

	log.Info("Snapshot backup starting")

	switch ssb.Status.Phase {
	case "", velerov1api.SnapshotBackupPhaseNew:
		// Only process new items.
	default:
		log.Debug("Snapshot backup is not new, not processing")
		return ctrl.Result{}, nil
	}

	//Create pod and mount pvc

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ssb.Spec.BackupPvc,
			Namespace: ssb.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    ssb.Spec.BackupPvc,
					Image:   "gcr.io/velero-gcp/busybox",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      ssb.Spec.BackupPvc,
						MountPath: "/" + ssb.Spec.BackupPvc,
					}},
				},
			},
			Volumes: []corev1.Volume{{
				Name: ssb.Spec.BackupPvc,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: ssb.Spec.BackupPvc,
					},
				},
			}},
		},
	}

	if err := s.Client.Create(ctx, pod, &client.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return s.updateStatusToFailed(ctx, &ssb, err, fmt.Sprintf("error creating pod %s in namespace %s", pod.Name, pod.Namespace), log)
		}
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := wait.PollImmediate(PollInterval, PollTimeout, func() (done bool, err error) {
		if err := s.Client.Get(ctx, types.NamespacedName{
			Namespace: ssb.Namespace,
			Name:      ssb.Spec.BackupPvc,
		}, pvc); err != nil {
			return false, err
		} else {
			return pvc.Status.Phase == v1.ClaimBound, nil
		}
	}); err != nil {
		return s.updateStatusToFailed(ctx, &ssb, err, fmt.Sprintf("error with %v backup snapshot for pvc %s for its status %s is not in Bound", err, ssb.Spec.BackupPvc, pvc.Status.Phase), log)
	}

	podNamespacedName := client.ObjectKey{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}
	if err := wait.PollImmediate(PollInterval, PollTimeout, func() (done bool, err error) {
		var podErr error
		if pod, podErr = s.kubeClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); podErr != nil {
			return false, err
		} else {
			return pod.Status.Phase == v1.PodRunning, nil

		}
	}); err != nil {
		return s.updateStatusToFailed(ctx, &ssb, err, fmt.Sprintf("failed to wait for pod %v running but %v with error %v", podNamespacedName, pod.Status.Phase, err), log)
	}

	if pod.Spec.NodeName != s.NodeName { // Only process items for this node.
		return ctrl.Result{}, nil
	}

	defer func() {
		log.Debugf("deleting pod %s in namespace %s", pod.Name, pod.Namespace)
		var gracePeriodSeconds int64 = 0
		if err := s.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds}); err != nil {
			s.updateStatusToFailed(ctx, &ssb, err, fmt.Sprintf("error delete pod %s in namespace %s", pod.Name, pod.Namespace), log)
		}
	}()

	// Update status to InProgress.
	original := ssb.DeepCopy()
	ssb.Status.Phase = velerov1api.SnapshotBackupPhaseInProgress
	ssb.Status.StartTimestamp = &metav1.Time{Time: s.Clock.Now()}
	if err := s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotBackup status")
		return ctrl.Result{}, err
	}

	volDir, err := kube.GetVolumeDirectory(ctx, log, pod, ssb.Spec.BackupPvc, s.Client)
	if err != nil {
		return s.updateStatusToFailed(ctx, &ssb, err, "getting volume directory name", log)
	}

	pathGlob := fmt.Sprintf("/host_pods/%s/volumes/*/%s", string(pod.GetUID()), volDir)
	log.WithField("pathGlob", pathGlob).Debug("Looking for path matching glob")

	path, err := kube.SinglePathMatch(pathGlob, s.FileSystem, log)
	if err != nil {
		return s.updateStatusToFailed(ctx, &ssb, err, "identifying unique volume path on host", log)
	}
	log.WithField("path", path).Debugf("Found path matching glob")

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := s.Client.Get(context.Background(), client.ObjectKey{
		Namespace: ssb.Namespace,
		Name:      ssb.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error getting backup storage location")
	}

	backupRepo, err := s.RepositoryEnsurer.EnsureRepo(ctx, ssb.Namespace, ssb.Namespace, ssb.Spec.BackupStorageLocation, ssb.Spec.UploaderType)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error ensure backup repository")
	}

	var uploaderProv provider.Provider
	uploaderProv, err = NewUploaderProviderFunc(ctx, s.Client, ssb.Spec.UploaderType, ssb.Spec.RepoIdentifier,
		backupLocation, backupRepo, s.CredentialGetter, repokey.RepoKeySelector(), log)
	if err != nil {
		return s.updateStatusToFailed(ctx, &ssb, err, "error creating uploader", log)
	}

	// If this is a PVC, look for the most recent completed pod volume backup for it and get
	// its snapshot ID to do new backup based on it. Without this,
	// if the pod using the PVC (and therefore the directory path under /host_pods/) has
	// changed since the PVC's last backup, for backup, it will not be able to identify a suitable
	// parent snapshot to use, and will have to do a full rescan of the contents of the PVC.
	var parentSnapshotID string
	if pvcUID, ok := ssb.Labels[velerov1api.PVCUIDLabel]; ok {
		parentSnapshotID = s.getParentSnapshot(ctx, log, pvcUID, &ssb)
		if parentSnapshotID == "" {
			log.Info("No parent snapshot found for PVC, not based on parent snapshot for this backup")
		} else {
			log.WithField("parentSnapshotID", parentSnapshotID).Info("Based on parent snapshot for this backup")
		}
	}

	defer func() {
		if err := uploaderProv.Close(ctx); err != nil {
			log.Errorf("failed to close uploader provider with error %v", err)
		}
	}()

	snapshotID, emptySnapshot, err := uploaderProv.RunBackup(ctx, path, ssb.Spec.Tags, parentSnapshotID, s.NewSnapshotBackupProgressUpdater(&ssb, log, ctx))
	if err != nil {
		return s.updateStatusToFailed(ctx, &ssb, err, fmt.Sprintf("running backup, stderr=%v", err), log)
	}

	// Update status to Completed with path & snapshot ID.
	original = ssb.DeepCopy()
	ssb.Status.Path = path
	ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCompleted
	ssb.Status.SnapshotID = snapshotID
	ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}
	if emptySnapshot {
		ssb.Status.Message = "volume was empty so no snapshot was taken"
	}
	if err = s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating PodVolumeBackup status")
		return ctrl.Result{}, err
	}
	log.Info("snapshot backup completed")
	return ctrl.Result{}, nil

}

// SetupWithManager registers the SnapshotBackup controller.
func (r *SnapshotBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.SnapshotBackup{}).
		Complete(r)
}

// getParentSnapshot finds the most recent completed PodVolumeBackup for the
// specified PVC and returns its snapshot ID. Any errors encountered are
// logged but not returned since they do not prevent a backup from proceeding.
func (r *SnapshotBackupReconciler) getParentSnapshot(ctx context.Context, log logrus.FieldLogger, pvcUID string, ssb *velerov1api.SnapshotBackup) string {
	log = log.WithField("pvcUID", pvcUID)
	log.Infof("Looking for most recent completed SnapshotBackup for this PVC")

	listOpts := &client.ListOptions{
		Namespace: ssb.Namespace,
	}
	matchingLabels := client.MatchingLabels(map[string]string{velerov1api.PVCUIDLabel: pvcUID})
	matchingLabels.ApplyToList(listOpts)

	var ssbList velerov1api.SnapshotBackupList
	if err := r.Client.List(ctx, &ssbList, listOpts); err != nil {
		log.WithError(errors.WithStack(err)).Error("getting list of SnapshotBackups for this PVC")
	}

	// Go through all the podvolumebackups for the PVC and look for the most
	// recent completed one to use as the parent.
	var mostRecentSSB velerov1api.SnapshotBackup
	for _, ssbItem := range ssbList.Items {
		if ssbItem.Spec.UploaderType != ssb.Spec.UploaderType {
			continue
		}
		if ssbItem.Status.Phase != velerov1api.SnapshotBackupPhaseCompleted {
			continue
		}

		if ssbItem.Spec.BackupStorageLocation != ssb.Spec.BackupStorageLocation {
			// Check the backup storage location is the same as spec in order to
			// support backup to multiple backup-locations. Otherwise, there exists
			// a case that backup volume snapshot to the second location would
			// failed, since the founded parent ID is only valid for the first
			// backup location, not the second backup location. Also, the second
			// backup should not use the first backup parent ID since its for the
			// first backup location only.
			continue
		}

		if mostRecentSSB.Status == (velerov1api.SnapshotBackupStatus{}) || ssb.Status.StartTimestamp.After(mostRecentSSB.Status.StartTimestamp.Time) {
			mostRecentSSB = ssbItem
		}
	}

	if mostRecentSSB.Status == (velerov1api.SnapshotBackupStatus{}) {
		log.Info("No completed SnapshotBackup found for PVC")
		return ""
	}

	log.WithFields(map[string]interface{}{
		"parentSnapshotBackup": mostRecentSSB.Name,
		"parentSnapshotID":     mostRecentSSB.Status.SnapshotID,
	}).Info("Found most recent completed SnapshotBackup for PVC")

	return mostRecentSSB.Status.SnapshotID
}

func (s *SnapshotBackupReconciler) updateStatusToFailed(ctx context.Context, ssb *velerov1api.SnapshotBackup, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	original := ssb.DeepCopy()
	ssb.Status.Phase = velerov1api.SnapshotBackupPhaseFailed
	ssb.Status.Message = errors.WithMessage(err, msg).Error()
	if ssb.Status.StartTimestamp.IsZero() {
		ssb.Status.StartTimestamp = &metav1.Time{Time: s.Clock.Now()}
	}
	ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}
	if err = s.Client.Patch(ctx, ssb, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotBackup status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (s *SnapshotBackupReconciler) NewSnapshotBackupProgressUpdater(ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger, ctx context.Context) *SnapshotBackupProgressUpdater {
	return &SnapshotBackupProgressUpdater{ssb, log, ctx, s.Client}
}

//UpdateProgress which implement ProgressUpdater interface to update snapshot backup progress status
func (s *SnapshotBackupProgressUpdater) UpdateProgress(p *uploader.UploaderProgress) {
	original := s.SnapshotBackup.DeepCopy()
	s.SnapshotBackup.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot %s backup progress with uninitailize client", s.SnapshotBackup.Spec.BackupPvc)
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotBackup, client.MergeFrom(original)); err != nil {
		s.Log.Errorf("update backup snapshot %s  progress with %v", s.SnapshotBackup.Spec.BackupPvc, err)
	}
}
