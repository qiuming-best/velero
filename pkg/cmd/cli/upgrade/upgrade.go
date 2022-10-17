package upgrade

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vmware-tanzu/velero/internal/velero"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/cmd"
	"github.com/vmware-tanzu/velero/pkg/cmd/cli/version"
	"github.com/vmware-tanzu/velero/pkg/cmd/util/output"
	"github.com/vmware-tanzu/velero/pkg/deploy"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"golang.org/x/mod/semver"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// UpgradeOptions collects all the options for upgrading Velero from an old version.
type UpgradeOptions struct {
	Namespace     string
	Image         string
	ClientVersion string
	ServerVersion string
	UploaderType  string
	Wait          bool
}

// BindFlags adds command line values to the options struct.
func (o *UpgradeOptions) BindFlags(flags *pflag.FlagSet) {
	flags.StringVarP(&o.Namespace, "namespace", "n", o.Namespace, "Namespace of Velero server deployed. Optional.")
	flags.StringVar(&o.Image, "image", o.Image, "Image upgraded for the Velero and node agent server pods. Optional.")
	flags.StringVar(&o.UploaderType, "uploader-type", o.UploaderType, fmt.Sprintf("The type of uploader to be upgrade, the supported values are '%s', '%s'", uploader.ResticType, uploader.KopiaType))
	flags.BoolVar(&o.Wait, "wait", o.Wait, "Wait for Velero deployment upgration to be ready. Optional.")
}

// NewUpgradeOptions instantiates a new, default UpgradeOptions struct.
func NewUpgradeOptions() *UpgradeOptions {
	return &UpgradeOptions{
		Namespace: velerov1api.DefaultNamespace,
		Image:     velero.DefaultVeleroImage(),
	}
}

// NewCommand creates a cobra command.
func NewCommand(f client.Factory) *cobra.Command {
	o := NewUpgradeOptions()
	dynamicClient, err := f.DynamicClient()
	cmd.CheckError(err)
	dynamicFactory := client.NewDynamicFactory(dynamicClient)
	kbClient, err := f.KubebuilderClient()
	cmd.CheckError(err)

	serverVersion, clientVersion, err := version.GetVersion(f, kbClient)
	o.ServerVersion = semver.MajorMinor(serverVersion)
	o.ClientVersion = semver.MajorMinor(clientVersion)
	cmd.CheckError(err)

	c := &cobra.Command{
		Use:     "upgrade",
		Short:   "Upgrade Velero",
		Long:    "Upgrade Velero version from current environment version into specific version",
		Example: ``,
		Run: func(c *cobra.Command, args []string) {
			cmd.CheckError(o.Validate(c, args, f))
			cmd.CheckError(o.Complete(args, f))
			cmd.CheckError(o.Run(c, dynamicFactory, kbClient))
		},
	}
	o.BindFlags(c.Flags())
	output.BindFlags(c.Flags())
	output.ClearOutputFlagDefault(c)
	return c
}

// Run executes a command in the context of the provided arguments.
func (o *UpgradeOptions) Run(c *cobra.Command, f client.DynamicFactory, kbClient kbclient.Client) error {
	errorMsg := fmt.Sprintf("\n\nError upgrading Velero. Use `kubectl logs deploy/velero -n %s` to check the deploy logs", o.Namespace)
	// Get resources
	resources, err := GetUpgradeResources(f, o)
	if err != nil {
		return errors.Wrap(err, errorMsg)
	}
	// Delete resources
	daemonSet, err := deploy.GetOriginalDaemonSet(f, "restic", o.Namespace)
	if err != nil {
		return errors.Wrap(err, errorMsg)
	}
	daemonSetResources := new(unstructured.UnstructuredList)
	daemonSetResources.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "List"})
	deploy.AppendUnstructured(daemonSetResources, daemonSet)

	if err := deploy.Delete(f, kbClient, daemonSetResources, os.Stdout); err != nil {
		return errors.Wrap(err, errorMsg)
	} else {
		fmt.Println("Older Velero restic deamonset is to be deleted.")
	}

	// Create resources
	if err := deploy.Install(f, kbClient, resources, os.Stdout); err != nil {
		return errors.Wrap(err, errorMsg)
	}

	if o.Wait {
		fmt.Println("Waiting for Velero deployment to be ready.")
		if _, err := deploy.DeploymentIsReady(f, o.Namespace); err != nil {
			return errors.Wrap(err, errorMsg)
		}

		fmt.Println("Waiting for node-agent daemonset to be ready.")
		if _, err := deploy.DaemonSetIsReady(f, "node-agent", o.Namespace); err != nil {
			return errors.Wrap(err, errorMsg)
		}
	}

	fmt.Printf("Velero is upgraded! ⛵ Use 'kubectl logs deployment/velero -n %s' to view the status.\n", o.Namespace)
	return nil

}

// Validate validates options provided to a command.
func (o *UpgradeOptions) Validate(c *cobra.Command, args []string, f client.Factory) error {
	if err := output.ValidateFlags(c); err != nil {
		return err
	}
	if o.ServerVersion == o.ClientVersion {
		return errors.Errorf("client Version %s equal with Server Version %s so it will not do upgrade", o.ClientVersion, o.ServerVersion)
	} else if o.ServerVersion < o.ClientVersion {
		return errors.New("downgrade is not supported currently")
	}
	if err := uploader.ValidateUploaderType(o.UploaderType); err != nil {
		return err
	}
	return nil
}

//Complete completes options for a command.
func (o *UpgradeOptions) Complete(args []string, f client.Factory) error {
	o.Namespace = f.Namespace()
	return nil
}

func GetUpgradeResources(factory client.DynamicFactory, opt *UpgradeOptions) (*unstructured.UnstructuredList, error) {
	// For crds
	resources := deploy.AllCRDs()

	// For deployment
	deployment, err := deploy.GetOriginalDeployment(factory, opt.Namespace)
	if err != nil {
		return nil, errors.Wrap(err, "error getting velero deployment from unstructured")
	}
	// Apply changes
	deployOpts := []deploy.PodTemplateOption{deploy.WithImage(opt.Image)}
	if opt.ServerVersion > "v1.9" {
		deployOpts = append(deployOpts, deploy.WithUploaderType(opt.UploaderType))
		deployOpts = append(deployOpts, deploy.WithDefaultVolumesToFsBackup())
	}
	deployment = deploy.PatchDeployment(opt.ServerVersion, deployment, deployOpts...)
	deploy.AppendUnstructured(resources, deployment)

	// For daemonset
	daemonSet, err := deploy.GetOriginalDaemonSet(factory, "restic", opt.Namespace)
	if err != nil {
		return nil, errors.Wrap(err, "error getting velero daemonSet from unstructured")
	}
	// Apply changes
	dsOpts := []deploy.PodTemplateOption{
		deploy.WithImage(opt.Image),
	}
	daemonSet = deploy.PatchDaemonset(opt.ServerVersion, daemonSet, dsOpts...)
	deploy.AppendUnstructured(resources, daemonSet)
	return resources, nil
}
