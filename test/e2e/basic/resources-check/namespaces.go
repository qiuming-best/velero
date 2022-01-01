/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the Licensm.
You may obtain a copy of the License at

    http://www.apachm.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the Licensm.
*/

package basic

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/vmware-tanzu/velero/test/e2e"
	. "github.com/vmware-tanzu/velero/test/e2e/test"
	. "github.com/vmware-tanzu/velero/test/e2e/util/k8s"
)

type MultiNSBackup struct {
	TestCase
	IsScalTest      bool
	NSExcluded      *[]string
	TimeoutDuration time.Duration
}

func (m *MultiNSBackup) Init() error {
	rand.Seed(time.Now().UnixNano())
	UUIDgen, _ = uuid.NewRandom()
	m.BackupName = "backup-" + UUIDgen.String()
	m.RestoreName = "restore-" + UUIDgen.String()
	m.NSBaseName = "nstest-" + UUIDgen.String()
	m.Client = TestClientInstance
	m.NSExcluded = &[]string{}

	// Currently it's hard to build a large list of namespaces to include and wildcards do not work so instead
	// we will exclude all of the namespaces that existed prior to the test from the backup
	namespaces, err := m.Client.ClientGo.CoreV1().Namespaces().List(context.Background(), v1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "Could not retrieve namespaces")
	}

	for _, excludeNamespace := range namespaces.Items {
		*m.NSExcluded = append(*m.NSExcluded, excludeNamespace.Name)
	}

	if m.IsScalTest {
		m.NamespacesTotal = 2500
		m.TimeoutDuration = time.Hour * 5
		m.TestMsg = &TestMSG{
			Text:      "When I create 2500 namespaces should be successfully backed up and restored",
			FailedMSG: "Failed to successfully backup and restore multiple namespaces",
		}
	} else {
		m.NamespacesTotal = 2
		m.TimeoutDuration = time.Minute * 60
		m.TestMsg = &TestMSG{
			Text:      "When I create 2 namespaces should be successfully backed up and restored",
			FailedMSG: "Failed to successfully backup and restore multiple namespaces",
		}
	}

	m.BackupArgs = []string{
		"create", "--namespace", VeleroCfg.VeleroNamespace, "backup", m.BackupName,
		"--exclude-namespaces", strings.Join(*m.NSExcluded, ","),
		"--default-volumes-to-restic", "--wait",
	}

	m.RestoreArgs = []string{
		"create", "--namespace", VeleroCfg.VeleroNamespace, "restore", m.RestoreName,
		"--from-backup", m.BackupName, "--wait",
	}

	return nil
}

/*func (m *MultiNSBackup) CreateResources() error {
	m.Ctx, _ = context.WithTimeout(context.Background(), m.TimeoutDuration)
	fmt.Printf("Creating namespaces ...\n")
	wg := sync.WaitGroup{}
	concurrency := 10
	waitCh := make(chan struct{})
	errChan := make(chan error, concurrency)
	defer close(errChan)
	labels := map[string]string{
		"ns-test": "true",
	}
	go func() {
		for nsNum := 0; nsNum < m.NamespacesTotal; nsNum++ {
			createNSName := fmt.Sprintf("%s-%00000d", m.NSBaseName, nsNum)
			fmt.Printf("Creating %d %s namespaces ...\n", nsNum, createNSName)
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err error
				if err = CreateNamespaceWithLabel(m.Ctx, m.Client, createNSName, labels); err != nil {
					err = errors.Wrapf(err, "Failed to create namespace %s", createNSName)
				}
				errChan <- err
			}()
		}
		wg.Wait()
		close(waitCh)
	}()

	deadline, ok := m.Ctx.Deadline()
	if !ok {
		return errors.New("faild to get context deadline when create namespaces")
	}

	for {
		select {
		case err := <-errChan:
			if err != nil {
				return err
			}
		case <-time.After(time.Until(deadline)):
			return errors.New("faild to create namespaces with timeout")
		case <-waitCh:
			return nil
		}
	}
}*/

func (m *MultiNSBackup) CreateResources() error {
	m.Ctx, _ = context.WithTimeout(context.Background(), m.TimeoutDuration)
	labels := map[string]string{
		"ns-test": "true",
	}
	for nsNum := 0; nsNum < m.NamespacesTotal; nsNum++ {
		createNSName := fmt.Sprintf("%s-%00000d", m.NSBaseName, nsNum)
		fmt.Printf("Creating %d %s namespaces ...\n", nsNum, createNSName)

		if err := CreateNamespaceWithLabel(m.Ctx, m.Client, createNSName, labels); err != nil {
			return errors.Wrapf(err, "Failed to create namespace %s", createNSName)
		}
	}
	return nil
}

func (m *MultiNSBackup) Verify() error {
	// Verify that we got back all of the namespaces we created
	for nsNum := 0; nsNum < m.NamespacesTotal; nsNum++ {
		checkNSName := fmt.Sprintf("%s-%00000d", m.NSBaseName, nsNum)
		checkNS, err := GetNamespace(m.Ctx, m.Client, checkNSName)
		fmt.Printf("Verify %d %s namespaces ...\n", nsNum, checkNSName)
		if err != nil {
			return errors.Wrapf(err, "Could not retrieve test namespace %s", checkNSName)
		} else if checkNS.Name != checkNSName {
			return errors.Errorf("Retrieved namespace for %s has name %s instead", checkNSName, checkNS.Name)
		}
	}
	return nil
}

/*func (m *MultiNSBackup) Verify() error {
	// Verify that we got back all of the namespaces we created
	wg := sync.WaitGroup{}
	concurrency := 10
	waitCh := make(chan struct{})
	errChan := make(chan error, concurrency)
	defer close(errChan)
	go func() {
		for nsNum := 0; nsNum < m.NamespacesTotal; nsNum++ {
			checkNSName := fmt.Sprintf("%s-%00000d", m.NSBaseName, nsNum)
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				var err error
				time.Sleep(time.Millisecond * 100)
				checkNS, err := GetNamespace(m.Ctx, m.Client, checkNSName)
				fmt.Printf("Verify %d %s namespaces ...\n", n, checkNSName)
				if err != nil {
					err = errors.Wrapf(err, "Could not retrieve test namespace %s", checkNSName)
				} else if checkNS.Name != checkNSName {
					err = errors.Errorf("Retrieved namespace for %s has name %s instead", checkNSName, checkNS.Name)
				}
				errChan <- err
			}(nsNum)
		}
		wg.Wait()
		close(waitCh)
	}()

	deadline, ok := m.Ctx.Deadline()
	if !ok {
		return errors.New("faild to get context deadline when verify namespaces")
	}

	for {
		select {
		case err := <-errChan:
			if err != nil {
				return err
			}
		case <-time.After(time.Until(deadline)):
			return errors.New("faild to verify namespaces with timeout")
		case <-waitCh:
			return nil
		}
	}
}*/

func (m *MultiNSBackup) Destroy() error {
	err := CleanupNamespaces(m.Ctx, m.Client, m.NSBaseName)
	if err != nil {
		return errors.Wrap(err, "Could cleanup retrieve namespaces")
	}
	return WaitAllNamespacesDeleted(m.Ctx, m.Client, "ns-test=true")
}
