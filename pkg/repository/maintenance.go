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

package repository

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/vmware-tanzu/velero/pkg/repository/provider"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const RepositoryNameLabel = "velero.io/repo-name"

// MaintenanceConfig is the configuration for the repo maintenance job
type MaintenanceConfig struct {
	KeepLatestMaitenanceJobs int
	CPURequest               string
	MemoryRequest            string
	CPULimit                 string
	MemoryLimit              string
	LogLevelFlag             *logging.LevelFlag
	FormatFlag               *logging.FormatFlag
}

func generateJobName(repo string) string {
	millisecond := time.Now().UTC().UnixMilli() // milisecond

	jobName := fmt.Sprintf("%s-maintain-job-%d", repo, millisecond)
	if len(jobName) > 63 { // k8s job name length limit
		jobName = fmt.Sprintf("repo-maintain-job-%d", millisecond)
	}

	return jobName
}

// getEnvVarsFromVeleroServer Get the environment variables from the Velero server deployment
func getEnvVarsFromVeleroServer(deployment *appsv1.Deployment) []v1.EnvVar {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		// We only have one container in the Velero server deployment
		return container.Env
	}
	return nil
}

// getVolumeMountsFromVeleroServer Get the volume mounts from the Velero server deployment
func getVolumeMountsFromVeleroServer(deployment *appsv1.Deployment) []v1.VolumeMount {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		// We only have one container in the Velero server deployment
		return container.VolumeMounts
	}
	return nil
}

// getVolumesFromVeleroServer Get the volumes from the Velero server deployment
func getVolumesFromVeleroServer(deployment *appsv1.Deployment) []v1.Volume {
	return deployment.Spec.Template.Spec.Volumes
}

// getServiceAccountFromVeleroServer Get the service account from the Velero server deployment
func getServiceAccountFromVeleroServer(deployment *appsv1.Deployment) string {
	return deployment.Spec.Template.Spec.ServiceAccountName
}

// getVeleroServerDeployment Get the Velero server deployment
func getVeleroServerDeployment(cli client.Client, namespace string) (*appsv1.Deployment, error) {
	// Get the Velero server deployment
	deployment := appsv1.Deployment{}
	err := cli.Get(context.TODO(), types.NamespacedName{Name: "velero", Namespace: namespace}, &deployment)
	if err != nil {
		return nil, err
	}
	return &deployment, nil
}

func getVeleroServerImage(deployment *appsv1.Deployment) string {
	// Get the image of the Velero server deployment
	return deployment.Spec.Template.Spec.Containers[0].Image
}

func getResourceRequirements(cpuRequest, memoryRequest, cpuLimit, memoryLimit string) v1.ResourceRequirements {
	res := v1.ResourceRequirements{}
	if cpuRequest != "" {
		res.Requests[v1.ResourceCPU] = resource.MustParse(cpuRequest)
	}
	if memoryRequest != "" {
		res.Requests[v1.ResourceMemory] = resource.MustParse(memoryRequest)
	}
	if cpuLimit != "" {
		res.Limits[v1.ResourceCPU] = resource.MustParse(cpuLimit)
	}
	if memoryLimit != "" {
		res.Limits[v1.ResourceMemory] = resource.MustParse(memoryLimit)
	}
	return res
}

func buildMaintenanceJob(m MaintenanceConfig, param provider.RepoParam, cli client.Client, namespace string) (*batchv1.Job, error) {
	// Get the Velero server deployment
	deployment, err := getVeleroServerDeployment(cli, namespace)
	if err != nil {
		return nil, err
	}

	// Get the environment variables from the Velero server deployment
	envVars := getEnvVarsFromVeleroServer(deployment)

	// Get the volume mounts from the Velero server deployment
	volumeMounts := getVolumeMountsFromVeleroServer(deployment)

	// Get the volumes from the Velero server deployment
	volumes := getVolumesFromVeleroServer(deployment)

	// Get the service account from the Velero server deployment
	serviceAccount := getServiceAccountFromVeleroServer(deployment)

	// Get image
	image := getVeleroServerImage(deployment)

	// Set resource limits and requests
	resources := getResourceRequirements(m.CPURequest, m.MemoryRequest, m.CPULimit, m.MemoryLimit)

	// Set arguments
	args := []string{"server", "repo-maintenance"}
	args = append(args, fmt.Sprintf("--repo-name=%s", param.BackupRepo.Spec.VolumeNamespace))
	args = append(args, fmt.Sprintf("--repo-type=%s", param.BackupRepo.Spec.RepositoryType))
	args = append(args, fmt.Sprintf("--backup-storage-location=%s", param.BackupLocation.Name))
	args = append(args, fmt.Sprintf("--log-level=%s", m.LogLevelFlag.String()))
	args = append(args, fmt.Sprintf("--log-format=%s", m.FormatFlag.String()))

	// build the maintenance job
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateJobName(param.BackupRepo.Name),
			Namespace: param.BackupRepo.Namespace,
			Labels: map[string]string{
				RepositoryNameLabel: param.BackupRepo.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32), // Never retry
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: "velero-repo-maintenance-pod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "velero-repo-maintenance-container",
							Image: image,
							Command: []string{
								"/velero",
							},
							Args:            args,
							ImagePullPolicy: v1.PullIfNotPresent,
							Env:             envVars,
							VolumeMounts:    volumeMounts,
							Resources:       resources,
						},
					},
					RestartPolicy:      v1.RestartPolicyNever,
					Volumes:            volumes,
					ServiceAccountName: serviceAccount,
				},
			},
		},
	}, nil
}

// deleteOldMaintenanceJobs deletes old maintenance jobs and keeps the latest N jobs
func deleteOldMaintenanceJobs(cli client.Client, repo string, keep int) error {
	// Get the maintenance job list by label
	jobList := &batchv1.JobList{}
	err := cli.List(context.TODO(), jobList, client.MatchingLabels(map[string]string{RepositoryNameLabel: repo}))
	if err != nil {
		return err
	}

	// Delete old maintenance jobs
	if len(jobList.Items) > keep {
		sort.Slice(jobList.Items, func(i, j int) bool {
			return jobList.Items[i].CreationTimestamp.Before(&jobList.Items[j].CreationTimestamp)
		})
		for i := 0; i < len(jobList.Items)-keep; i++ {
			err = cli.Delete(context.TODO(), &jobList.Items[i])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func waitForJobComplete(ctx context.Context, client client.Client, job *batchv1.Job) error {
	return wait.PollImmediateUntil(1, func() (bool, error) {
		err := client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, job)
		if err != nil {
			return false, err
		}
		if job.Status.Succeeded > 0 {
			return true, nil
		}

		if job.Status.Failed > 0 {
			return true, fmt.Errorf("maintenance job %s failed", job.Name)
		}
		return false, nil
	}, ctx.Done())
}

func getMaintenanceResultFromJob(cli client.Client, job *batchv1.Job) (string, error) {
	// Get the maintenance job related pod by label selector
	podList := &v1.PodList{}
	err := cli.List(context.TODO(), podList, client.InNamespace(job.Namespace), client.MatchingLabels(map[string]string{"job-name": job.Name}))
	if err != nil {
		return "", err
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("no pod found for job %s", job.Name)
	}

	// we only have one maintenance pod for the job
	return podList.Items[0].Status.ContainerStatuses[0].State.Terminated.Message, nil

}

func GetLatestMaintenanceJob(cli client.Client, repo string) (*batchv1.Job, error) {
	// Get the maintenance job list by label
	jobList := &batchv1.JobList{}
	err := cli.List(context.TODO(), jobList, client.MatchingLabels(map[string]string{RepositoryNameLabel: repo}))
	if err != nil {
		return nil, err
	}

	if len(jobList.Items) == 0 {
		return nil, nil
	}

	// Get the latest maintenance job
	sort.Slice(jobList.Items, func(i, j int) bool {
		return jobList.Items[i].CreationTimestamp.Time.After(jobList.Items[j].CreationTimestamp.Time)
	})
	return &jobList.Items[0], nil
}
