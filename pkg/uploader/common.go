package uploader

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
)

const (
	// DaemonSet is the name of the Velero podVolumeProtect daemonset.
	DaemonSet = "restic"

	// DefaultVolumesToRestic specifies whether podVolumeProtect should be used, by default, to
	// take backup of all pod volumes.
	DefaultVolumesToRestic = false

	DefaultToLegacyUploader = true

	// InitContainer is the name of the init container added
	// to workload pods to help with restores.
	InitContainer = "restic-wait"

	// PVCNameAnnotation is the key for the annotation added to
	// podVolumeProtect when they're for a PVC.
	PVCNameAnnotation = "velero.io/pvc-name"

	// VolumesToBackupAnnotation is the annotation on a pod whose mounted volumes
	// need to be backed up using podVolumeProtect.
	VolumesToBackupAnnotation = "backup.velero.io/backup-volumes"

	// VolumesToExcludeAnnotation is the annotation on a pod whose mounted volumes
	// should be excluded from podVolumeProtect.
	VolumesToExcludeAnnotation = "backup.velero.io/backup-volumes-excludes"

	// Deprecated.
	//
	// TODO(2.0): remove
	podAnnotationPrefix = "snapshot.velero.io/"
)

type SnapshotInfo struct {
	snapshotID   string
	snapshotSize int64
}

// getPodSnapshotAnnotations returns a map, of volume name -> snapshot id,
// of all repository snapshots for this pod.
// TODO(2.0) to remove
// Deprecated: we will stop using pod annotations to record repository snapshot IDs after they're taken,
// therefore we won't need to check if these annotations exist.
func getPodSnapshotAnnotations(obj metav1.Object) map[string]string {
	var res map[string]string

	insertSafe := func(k, v string) {
		if res == nil {
			res = make(map[string]string)
		}
		res[k] = v
	}

	for k, v := range obj.GetAnnotations() {
		if strings.HasPrefix(k, podAnnotationPrefix) {
			insertSafe(k[len(podAnnotationPrefix):], v)
		}
	}

	return res
}

func isPVBMatchPod(pvb *velerov1api.PodVolumeBackup, podName string, namespace string) bool {
	return podName == pvb.Spec.Pod.Name && namespace == pvb.Spec.Pod.Namespace
}

// volumeHasNonRestorableSource checks if the given volume exists in the list of podVolumes
// and returns true if the volume's source is not restorable. This is true for volumes with
// a Projected or DownwardAPI source.
func volumeHasNonRestorableSource(volumeName string, podVolumes []corev1api.Volume) bool {
	var volume corev1api.Volume
	for _, v := range podVolumes {
		if v.Name == volumeName {
			volume = v
			break
		}
	}
	return volume.Projected != nil || volume.DownwardAPI != nil
}

// GetVolumeBackupsForPod returns a map, of volume name -> snapshot id,
// of the PodVolumeBackups that exist for the provided pod.
func GetVolumeBackupsForPod(podVolumeBackups []*velerov1api.PodVolumeBackup, pod *corev1api.Pod, sourcePodNs string) map[string]string {
	volumes := make(map[string]string)

	for _, pvb := range podVolumeBackups {
		if !isPVBMatchPod(pvb, pod.GetName(), sourcePodNs) {
			continue
		}

		// skip PVBs without a snapshot ID since there's nothing
		// to restore (they could be failed, or for empty volumes).
		if pvb.Status.SnapshotID == "" {
			continue
		}

		// If the volume came from a projected or DownwardAPI source, skip its restore.
		// This allows backups affected by https://github.com/vmware-tanzu/velero/issues/3863
		// or https://github.com/vmware-tanzu/velero/issues/4053 to be restored successfully.
		if volumeHasNonRestorableSource(pvb.Spec.Volume, pod.Spec.Volumes) {
			continue
		}

		volumes[pvb.Spec.Volume] = pvb.Status.SnapshotID
	}

	if len(volumes) > 0 {
		return volumes
	}

	return getPodSnapshotAnnotations(pod)
}

// GetPodVolumesUsingPodVolumeProtect returns a list of volume names to backup for the provided pod.
func GetPodVolumesUsingPodVolumeProtect(pod *corev1api.Pod, defaultVolumesToFsUploader bool) []string {
	if !defaultVolumesToFsUploader {
		return GetVolumesToBackup(pod)
	}

	volsToExclude := getVolumesToExclude(pod)
	podVolumes := []string{}
	for _, pv := range pod.Spec.Volumes {
		// cannot backup hostpath volumes as they are not mounted into /var/lib/kubelet/pods
		// and therefore not accessible to the uploader.
		if pv.HostPath != nil {
			continue
		}
		// don't backup volumes mounting secrets. Secrets will be backed up separately.
		if pv.Secret != nil {
			continue
		}
		// don't backup volumes mounting config maps. Config maps will be backed up separately.
		if pv.ConfigMap != nil {
			continue
		}
		// don't backup volumes mounted as projected volumes, all data in those come from kube state.
		if pv.Projected != nil {
			continue
		}
		// don't backup DownwardAPI volumes, all data in those come from kube state.
		if pv.DownwardAPI != nil {
			continue
		}
		// don't backup volumes that are included in the exclude list.
		if contains(volsToExclude, pv.Name) {
			continue
		}
		// don't include volumes that mount the default service account token.
		if strings.HasPrefix(pv.Name, "default-token") {
			continue
		}
		podVolumes = append(podVolumes, pv.Name)
	}
	return podVolumes
}

// GetVolumesToBackup returns a list of volume names to backup for
// the provided pod.
// Deprecated: Use GetPodVolumesUsingPodVolumeProtect instead.
func GetVolumesToBackup(obj metav1.Object) []string {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return nil
	}

	backupsValue := annotations[VolumesToBackupAnnotation]
	if backupsValue == "" {
		return nil
	}

	return strings.Split(backupsValue, ",")
}

func getVolumesToExclude(obj metav1.Object) []string {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return nil
	}

	return strings.Split(annotations[VolumesToExcludeAnnotation], ",")
}

func contains(list []string, k string) bool {
	for _, i := range list {
		if i == k {
			return true
		}
	}
	return false
}

// NewPodVolumeRestoreListOptions creates a ListOptions with a label selector configured to
// find PodVolumeRestores for the restore identified by name.
func NewPodVolumeRestoreListOptions(name string) metav1.ListOptions {
	return metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", velerov1api.RestoreNameLabel, label.GetValidName(name)),
	}
}

// SnapshotIdentifier uniquely identifies a repository snapshot
// taken by Velero.
type SnapshotIdentifier struct {
	// VolumeNamespace is the namespace of the pod/volume that
	// the repository snapshot is for.
	VolumeNamespace string

	// BackupStorageLocation is the backup's storage location
	// name.
	BackupStorageLocation string

	// SnapshotID is the short ID of the repository snapshot.
	SnapshotID string
}

// GetSnapshotsInBackup returns a list of all repository snapshot ids associated with
// a given Velero backup.
func GetSnapshotsInBackup(ctx context.Context, backup *velerov1api.Backup, kbClient client.Client) ([]SnapshotIdentifier, error) {
	podVolumeBackups := &velerov1api.PodVolumeBackupList{}
	options := &client.ListOptions{
		LabelSelector: labels.Set(map[string]string{
			velerov1api.BackupNameLabel: label.GetValidName(backup.Name),
		}).AsSelector(),
	}

	err := kbClient.List(ctx, podVolumeBackups, options)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var res []SnapshotIdentifier
	for _, item := range podVolumeBackups.Items {
		if item.Status.SnapshotID == "" {
			continue
		}
		res = append(res, SnapshotIdentifier{
			VolumeNamespace:       item.Spec.Pod.Namespace,
			BackupStorageLocation: backup.Spec.StorageLocation,
			SnapshotID:            item.Status.SnapshotID,
		})
	}

	return res, nil
}
