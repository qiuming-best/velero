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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	corev1api "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository"
	repokey "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/provider"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

func NewSnapshotRestoreReconciler(logger logrus.FieldLogger, client client.Client, kubeClient kubernetes.Interface, credentialGetter *credentials.CredentialGetter, nodeName string) *SnapshotRestoreReconciler {
	return &SnapshotRestoreReconciler{
		Client:            client,
		kubeClient:        kubeClient,
		logger:            logger.WithField("controller", "SnapshotRestore"),
		credentialGetter:  credentialGetter,
		fileSystem:        filesystem.NewFileSystem(),
		clock:             &clock.RealClock{},
		nodeName:          nodeName,
		repositoryEnsurer: repository.NewRepositoryEnsurer(client, logger),
	}
}

type SnapshotRestoreReconciler struct {
	client.Client
	kubeClient        kubernetes.Interface
	logger            logrus.FieldLogger
	credentialGetter  *credentials.CredentialGetter
	fileSystem        filesystem.Interface
	clock             clock.Clock
	nodeName          string
	repositoryEnsurer *repository.RepositoryEnsurer
}

type SnapshotRestoreProgressUpdater struct {
	SnapshotRestore *velerov1api.SnapshotRestore
	Log             logrus.FieldLogger
	Ctx             context.Context
	Cli             client.Client
}

// +kubebuilder:rbac:groups=velero.io,resources=snapshotrestore,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=snapshotrestore/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumerclaims,verbs=get

func (s *SnapshotRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := s.logger.WithField("SnapshotRestore", req.NamespacedName.String())

	ssr := &velerov1api.SnapshotRestore{}

	if err := s.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, ssr); err != nil {
		if apierrors.IsNotFound(err) {
			log.Warn("SnapshotRestore not found, skip")
			return ctrl.Result{}, nil
		}
		log.WithError(err).Error("Unable to get the SnapshotRestore")
		return ctrl.Result{}, err
	}

	log.Info("Snapshot restore starting with phase %v", ssr.Status.Phase)
	switch ssr.Status.Phase {
	case "", velerov1api.SnapshotRestorePhaseNew:
		// Only process new items.
	default:
		log.Debug("Snapshot restore is not new, not processing")
		return ctrl.Result{}, nil
	}

	//Create pod and mount pvc

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ssr.Spec.RestorePvc,
			Namespace: ssr.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    ssr.Spec.RestorePvc,
					Image:   "gcr.io/velero-gcp/busybox",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      ssr.Spec.RestorePvc,
						MountPath: "/" + ssr.Spec.RestorePvc,
					}},
				},
			},
			Volumes: []corev1.Volume{{
				Name: ssr.Spec.RestorePvc,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: ssr.Spec.RestorePvc,
					},
				},
			}},
		},
	}
	if err := s.Client.Create(ctx, pod, &client.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("error creating pod %s in namespace %s", pod.Name, pod.Namespace), log)
		}
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := wait.PollImmediate(PollInterval, PollTimeout, func() (done bool, err error) {
		if err := s.Client.Get(ctx, types.NamespacedName{
			Namespace: ssr.Namespace,
			Name:      ssr.Spec.RestorePvc,
		}, pvc); err != nil {
			return false, err
		} else {
			return pvc.Status.Phase == v1.ClaimBound, nil
		}
	}); err != nil {
		return s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("error with %v backup snapshot for pvc %s for its status %s is not in Bound", err, ssr.Spec.RestorePvc, pvc.Status.Phase), log)
	}

	podNamespacedName := client.ObjectKey{
		Namespace: ssr.Namespace,
		Name:      ssr.Spec.RestorePvc,
	}

	if err := wait.PollImmediate(PollInterval, PollTimeout, func() (done bool, err error) {
		var podErr error
		if pod, podErr = s.kubeClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); podErr != nil {
			return false, err
		} else {
			return pod.Status.Phase == v1.PodRunning, nil

		}
	}); err != nil {
		return s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("failed to wait for pod %v running but %v with error %v", podNamespacedName, pod.Status.Phase, err), log)
	}

	if pod.Spec.NodeName != s.nodeName { // Only process items for this node.
		return ctrl.Result{}, nil
	}

	defer func() {
		log.Debugf("deleting pod %s in namespace %s", pod.Name, pod.Namespace)
		var gracePeriodSeconds int64 = 0
		if err := s.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds}); err != nil {
			s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("error delete pod %s in namespace %s", pod.Name, pod.Namespace), log)
		}
	}()

	original := ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseInProgress
	ssr.Status.StartTimestamp = &metav1.Time{Time: s.clock.Now()}
	if err := s.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("Unable to update status to in progress")
		return ctrl.Result{}, err
	}

	if err := s.processRestore(ctx, ssr, pod, log); err != nil {
		original = ssr.DeepCopy()
		ssr.Status.Phase = velerov1api.SnapshotRestorePhaseFailed
		ssr.Status.Message = err.Error()
		ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}
		if e := s.Patch(ctx, ssr, client.MergeFrom(original)); e != nil {
			log.WithError(err).Error("Unable to update status to failed")
		}

		log.WithError(err).Error("Unable to process the PodVolumeRestore")
		return ctrl.Result{}, err
	}

	original = ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseCompleted
	ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}
	if err := s.Patch(ctx, ssr.DeepCopy(), client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("Unable to update status to completed")
		return ctrl.Result{}, err
	}
	log.Info("Restore completed")
	return ctrl.Result{}, nil

}

func (s *SnapshotRestoreReconciler) processRestore(ctx context.Context, req *velerov1api.SnapshotRestore, pod *corev1api.Pod, log logrus.FieldLogger) error {
	volumeDir, err := kube.GetVolumeDirectory(ctx, log, pod, req.Spec.RestorePvc, s.Client)
	if err != nil {
		return errors.Wrap(err, "error getting volume directory name")
	}

	// Get the full path of the new volume's directory as mounted in the daemonset pod, which
	// will look like: /host_pods/<new-pod-uid>/volumes/<volume-plugin-name>/<volume-dir>
	volumePath, err := kube.SinglePathMatch(
		fmt.Sprintf("/host_pods/%s/volumes/*/%s", string(pod.UID), volumeDir),
		s.fileSystem, log)
	if err != nil {
		return errors.Wrap(err, "error identifying path of volume")
	}

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := s.Get(ctx, client.ObjectKey{
		Namespace: req.Namespace,
		Name:      req.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		return errors.Wrap(err, "error getting backup storage location")
	}

	backupRepo, err := s.repositoryEnsurer.EnsureRepo(ctx, req.Namespace, req.Spec.SourceNamespace, req.Spec.BackupStorageLocation, req.Spec.UploaderType)
	if err != nil {
		return errors.Wrap(err, "error ensure backup repository")
	}

	uploaderProv, err := provider.NewUploaderProvider(ctx, s.Client, req.Spec.UploaderType,
		req.Spec.RepoIdentifier, backupLocation, backupRepo, s.credentialGetter, repokey.RepoKeySelector(), log)
	if err != nil {
		return errors.Wrap(err, "error creating uploader")
	}

	defer func() {
		if err := uploaderProv.Close(ctx); err != nil {
			log.Errorf("failed to close uploader provider with error %v", err)
		}
	}()

	if err = uploaderProv.RunRestore(ctx, req.Spec.SnapshotID, volumePath, s.NewSnapshotBackupProgressUpdater(req, log, ctx)); err != nil {
		return errors.Wrapf(err, "error running restore err=%v", err)
	}

	return nil
}

func (s *SnapshotRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&velerov1api.SnapshotRestore{}).Complete(s)
}

func (s *SnapshotRestoreReconciler) updateStatusToFailed(ctx context.Context, ssr *velerov1api.SnapshotRestore, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	log.Infof("updateStatusToFailed %v", ssr.Status.Phase)
	original := ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseFailed
	ssr.Status.Message = errors.WithMessage(err, msg).Error()
	ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}

	if err = s.Client.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotRestore status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (s *SnapshotRestoreReconciler) NewSnapshotBackupProgressUpdater(ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger, ctx context.Context) *SnapshotRestoreProgressUpdater {
	return &SnapshotRestoreProgressUpdater{ssr, log, ctx, s.Client}
}

//UpdateProgress which implement ProgressUpdater interface to update pvr progress status
func (s *SnapshotRestoreProgressUpdater) UpdateProgress(p *uploader.UploaderProgress) {
	original := s.SnapshotRestore.DeepCopy()
	s.SnapshotRestore.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot %s restore progress with uninitailize client", s.SnapshotRestore.Spec.RestorePvc)
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotRestore, client.MergeFrom(original)); err != nil {
		s.Log.Errorf("update restore snapshot %s  progress with %v", s.SnapshotRestore.Spec.RestorePvc, err)
	}
}
