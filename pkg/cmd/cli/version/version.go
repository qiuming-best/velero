/*
Copyright 2020 the Velero contributors.

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

package version

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/mod/semver"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/pkg/buildinfo"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/cmd"
	"github.com/vmware-tanzu/velero/pkg/cmd/cli/serverstatus"
)

func NewCommand(f client.Factory) *cobra.Command {
	var clientOnly bool
	timeout := 5 * time.Second

	c := &cobra.Command{
		Use:   "version",
		Short: "Print the velero version and associated image",
		Run: func(c *cobra.Command, args []string) {
			var kbClient kbclient.Client
			if !clientOnly {
				var err error
				kbClient, err = f.KubebuilderClient()
				cmd.CheckError(err)
			}

			serverVersion, clientVersion, err := GetVersion(f, kbClient)
			if err != nil {
				fmt.Fprintf(os.Stdout, "<error getting server version: %s>\n", err)
				return
			}
			printVersion(os.Stdout, clientOnly, kbClient, serverVersion, clientVersion)
		},
	}

	c.Flags().DurationVar(&timeout, "timeout", timeout, "Maximum time to wait for server version to be reported. Default is 5 seconds.")
	c.Flags().BoolVar(&clientOnly, "client-only", clientOnly, "Only get velero client version, not server version")

	return c
}

func printVersion(w io.Writer, clientOnly bool, kbClient kbclient.Client, serverVersion, clientVersion string) {
	fmt.Fprintln(w, "Client:")
	fmt.Fprintf(w, "\tVersion: %s\n", buildinfo.Version)
	fmt.Fprintf(w, "\tGit commit: %s\n", buildinfo.FormattedGitSHA())

	if clientOnly {
		return
	}

	fmt.Fprintln(w, "Server:")
	fmt.Fprintf(w, "\tVersion: %s\n", serverVersion)

	serverSemVer := semver.MajorMinor(serverVersion)
	cliSemVer := semver.MajorMinor(buildinfo.Version)
	if serverSemVer != cliSemVer {
		upgrade := "client"
		cmp := semver.Compare(cliSemVer, serverSemVer)
		if cmp == 1 {
			upgrade = "server"
		}
		fmt.Fprintf(w, "# WARNING: the client version does not match the server version. Please update %s\n", upgrade)
	}
}

func GetVersion(f client.Factory, kbClient kbclient.Client) (string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serverStatusGetter := &serverstatus.DefaultServerStatusGetter{
		Namespace: f.Namespace(),
		Context:   ctx,
	}
	serverStatus, err := serverStatusGetter.GetServerStatus(kbClient)
	if err != nil {
		return "", "", err
	}
	return serverStatus.Status.ServerVersion, buildinfo.Version, nil
}
