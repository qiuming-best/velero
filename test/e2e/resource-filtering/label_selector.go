/*
Copyright 2021 the Velero contributors.

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

package filtering

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/vmware-tanzu/velero/test/e2e"
	. "github.com/vmware-tanzu/velero/test/e2e/test"
	. "github.com/vmware-tanzu/velero/test/e2e/util/k8s"
)

/*
Include resources matching the label selector.
	velero backup create <backup-name> --selector <key>=<value>
*/

type LabelSelector struct {
	FilteringCase
}

var BackupWithLabelSelector func() = TestFunc(&LabelSelector{testInBackup})

func (l *LabelSelector) Init() error {
	rand.Seed(time.Now().UnixNano())
	UUIDgen, _ = uuid.NewRandom()
	l.FilteringCase.Init()
	l.BackupName = "backup-label-selector-" + UUIDgen.String()
	l.RestoreName = "restore-label-selector-" + UUIDgen.String()
	l.NSBaseName = "backup-label-selector-" + UUIDgen.String()
	l.TestMsg = &TestMSG{
		Desc:      "Backup with the label selector test",
		Text:      "Should backup resources with selected label resource",
		FailedMSG: "Failed to backup resources with selected label",
	}
	l.labels = map[string]string{
		"resourcefiltering": "true",
	}
	l.labelSelector = "resourcefiltering"
	l.BackupArgs = []string{
		"create", "--namespace", VeleroCfg.VeleroNamespace, "backup", l.BackupName,
		"--selector", "resourcefiltering=true",
		"--default-volumes-to-restic", "--wait",
	}

	l.RestoreArgs = []string{
		"create", "--namespace", VeleroCfg.VeleroNamespace, "restore", l.RestoreName,
		"--from-backup", l.BackupName, "--wait",
	}
	return nil
}

func (l *LabelSelector) CreateResources() error {
	l.Ctx, _ = context.WithTimeout(context.Background(), 60*time.Minute)
	for nsNum := 0; nsNum < l.NamespacesTotal; nsNum++ {
		namespace := fmt.Sprintf("%s-%00000d", l.NSBaseName, nsNum)
		fmt.Printf("Creating resources in namespace ...%s\n", namespace)
		labels := l.labels
		if nsNum%2 == 0 {
			labels = map[string]string{
				"resourcefiltering": "false",
			}
		}
		if err := CreateNamespaceWithLabel(l.Ctx, l.Client, namespace, labels); err != nil {
			return errors.Wrapf(err, "Failed to create namespace %s", namespace)
		}

		//Create deployment
		fmt.Printf("Creating deployment in namespaces ...%s\n", namespace)

		deployment := NewDeployment(l.NSBaseName, namespace, l.replica, labels)
		deployment, err := CreateDeployment(l.Client.ClientGo, namespace, deployment)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to delete the namespace %q", namespace))
		}
		err = WaitForReadyDeployment(l.Client.ClientGo, namespace, deployment.Name)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to ensure job completion in namespace: %q", namespace))
		}
		//Create Secret
		secretName := l.NSBaseName
		fmt.Printf("Creating secret %s in namespaces ...%s\n", secretName, namespace)
		_, err = CreateSecret(l.Client.ClientGo, namespace, secretName, l.labels)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to create secret in the namespace %q", namespace))
		}
		err = WaitForSecretsComplete(l.Client.ClientGo, namespace, secretName)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to ensure secret completion in namespace: %q", namespace))
		}
	}
	return nil
}

func (l *LabelSelector) Verify() error {
	for nsNum := 0; nsNum < l.NamespacesTotal; nsNum++ {
		namespace := fmt.Sprintf("%s-%00000d", l.NSBaseName, nsNum)
		fmt.Printf("Checking resources in namespaces ...%s\n", namespace)
		//Check deployment
		_, err := GetDeployment(l.Client.ClientGo, namespace, l.NSBaseName)
		if nsNum%2 == 1 { //include
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to list deployment in namespace: %q", namespace))
			}
		} else { //exclude
			if err == nil {
				return fmt.Errorf("failed to exclude deployment in namespaces %q", namespace)
			} else {
				if apierrors.IsNotFound(err) { //resource should be excluded
					return nil
				}
				return errors.Wrap(err, fmt.Sprintf("failed to list deployment in namespace: %q", namespace))
			}
		}

		//Check secrets
		secretsList, err := l.Client.ClientGo.CoreV1().Secrets(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: l.labelSelector,
		})

		if nsNum%2 == 0 { //include
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to list secrets in namespace: %q", namespace))
			} else if len(secretsList.Items) == 0 {
				return errors.Errorf(fmt.Sprintf("no secrets found in namespace: %q", namespace))
			}
		} else { //exclude
			if err == nil {
				return fmt.Errorf("failed to exclude secrets in namespaces %q", namespace)
			} else {
				if apierrors.IsNotFound(err) { //resource should be excluded
					return nil
				}
				return errors.Wrap(err, fmt.Sprintf("failed to list secrets in namespace: %q", namespace))
			}
		}
	}
	return nil
}
