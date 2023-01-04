/*
Copyright the Velero contributors.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotBackupSpec is the specification for a SnapshotBackup.
type SnapshotBackupSpec struct {
	// BackupPvc is the name of the PVC for the snapshot to be backed up
	BackupPvc string `json:"backupPvc"`

	// BackupStorageLocation is the name of the backup storage location
	// where the backup repository is stored.
	BackupStorageLocation string `json:"backupStorageLocation"`

	// RepoIdentifier is the backup repository identifier.
	RepoIdentifier string `json:"repoIdentifier"`

	// UploaderType is the type of the uploader to handle the data transfer.
	// +kubebuilder:validation:Enum=kopia;restic;""
	// +optional
	UploaderType string `json:"uploaderType"`

	// Tags are a map of key-value pairs that should be applied to the
	// volume backup as tags.
	// +optional
	Tags map[string]string `json:"tags,omitempty"`
}

// SnapshotBackupPhase represents the lifecycle phase of a SnapshotBackup.
// +kubebuilder:validation:Enum=New;InProgress;Completed;Failed
type SnapshotBackupPhase string

const (
	SnapshotBackupPhaseNew        SnapshotBackupPhase = "New"
	SnapshotBackupPhaseInProgress SnapshotBackupPhase = "InProgress"
	SnapshotBackupPhaseCompleted  SnapshotBackupPhase = "Completed"
	SnapshotBackupPhaseFailed     SnapshotBackupPhase = "Failed"
)

// SnapshotBackupStatus is the current status of a SnapshotBackup.
type SnapshotBackupStatus struct {
	// Phase is the current state of the SnapshotBackup.
	// +optional
	Phase SnapshotBackupPhase `json:"phase,omitempty"`

	// Path is the full path of the snapshot volume being backed up.
	// +optional
	Path string `json:"path,omitempty"`

	// SnapshotID is the identifier for the snapshot in the backup repository.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`

	// Message is a message about the snapshot backup's status.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTimestamp records the time a backup was started.
	// Separate from CreationTimestamp, since that value changes
	// on restores.
	// The server's time is used for StartTimestamps
	// +optional
	// +nullable
	StartTimestamp *metav1.Time `json:"startTimestamp,omitempty"`

	// CompletionTimestamp records the time a backup was completed.
	// Completion time is recorded even on failed backups.
	// Completion time is recorded before uploading the backup object.
	// The server's time is used for CompletionTimestamps
	// +optional
	// +nullable
	CompletionTimestamp *metav1.Time `json:"completionTimestamp,omitempty"`

	// Progress holds the total number of bytes of the volume and the current
	// number of backed up bytes. This can be used to display progress information
	// about the backup operation.
	// +optional
	Progress DataMoveOperationProgress `json:"progress,omitempty"`
}

// TODO(2.0) After converting all resources to use the runttime-controller client,
// the genclient and k8s:deepcopy markers will no longer be needed and should be removed.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.phase",description="Snapshot Backup status such as New/InProgress"
// +kubebuilder:printcolumn:name="Created",type="date",JSONPath=".status.startTimestamp",description="Time when this backup was started"
// +kubebuilder:printcolumn:name="Volume",type="string",JSONPath=".spec.volume",description="Name of the volume to be backed up"
// +kubebuilder:printcolumn:name="Repository ID",type="string",JSONPath=".spec.repoIdentifier",description="Backup repository identifier for this backup"
// +kubebuilder:printcolumn:name="Uploader Type",type="string",JSONPath=".spec.uploaderType",description="The type of the uploader to handle data transfer"
// +kubebuilder:printcolumn:name="Storage Location",type="string",JSONPath=".spec.backupStorageLocation",description="Name of the Backup Storage Location where this backup should be stored"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:object:root=true
// +kubebuilder:object:generate=true

type SnapshotBackup struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec SnapshotBackupSpec `json:"spec,omitempty"`

	// +optional
	Status SnapshotBackupStatus `json:"status,omitempty"`
}

// TODO(2.0) After converting all resources to use the runtime-controller client,
// the k8s:deepcopy marker will no longer be needed and should be removed.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackups/status,verbs=get;update;patch

// SnapshotBackupList is a list of SnapshotBackups.
type SnapshotBackupList struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SnapshotBackup `json:"items"`
}
