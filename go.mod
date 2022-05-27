module github.com/vmware-tanzu/velero

go 1.16

require (
	cloud.google.com/go/storage v1.21.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-sdk-for-go v61.4.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/Azure/go-autorest/autorest v0.11.21
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.8
	github.com/Azure/go-autorest/autorest/to v0.3.0
	github.com/apex/log v1.9.0
	github.com/aws/aws-sdk-go v1.43.31
	github.com/bombsimon/logrusr v1.1.0
	github.com/evanphx/json-patch v4.11.0+incompatible
	github.com/fatih/color v1.13.0
	github.com/gobwas/glob v0.2.3
	github.com/gofrs/uuid v3.2.0+incompatible
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/hashicorp/go-hclog v0.14.1
	github.com/hashicorp/go-plugin v1.4.3
	github.com/joho/godotenv v1.3.0
	github.com/kubernetes-csi/external-snapshotter/client/v4 v4.2.0
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.16.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/robfig/cron v1.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/afero v1.6.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.1
	github.com/vmware-tanzu/crash-diagnostics v0.3.7
	golang.org/x/mod v0.5.1
	golang.org/x/net v0.0.0-20220325170049-de3da57026de
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.74.0
	google.golang.org/grpc v1.45.0
	k8s.io/api v0.22.2
	k8s.io/apiextensions-apiserver v0.22.2
	k8s.io/apimachinery v0.22.2
	k8s.io/cli-runtime v0.22.2
	k8s.io/client-go v0.22.2
	k8s.io/klog v1.0.0
	k8s.io/kube-aggregator v0.19.12
	sigs.k8s.io/cluster-api v1.0.0
	sigs.k8s.io/controller-runtime v0.10.2
	sigs.k8s.io/yaml v1.3.0
)

require github.com/kopia/kopia v0.10.7

require (
	github.com/Azure/go-autorest/autorest/validation v0.2.0 // indirect
	golang.org/x/text v0.3.7
)

replace github.com/gogo/protobuf => github.com/gogo/protobuf v1.3.2
