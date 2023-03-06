package resourcepolicies

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kbClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// ResPoliciesManager manage all resource policies in memory and keep all up-to-date
type ResPoliciesManager struct {
	resPolicies *ResourcePolicies
	namespace   string
	kbClient    kbClient.Client
}

var ResPoliciesMgr ResPoliciesManager

func (mgr *ResPoliciesManager) InitResPoliciesMgr(namespace string, kbClient kbClient.Client) {
	mgr.resPolicies = new(ResourcePolicies)
	mgr.namespace = namespace
	mgr.kbClient = kbClient
}

// getResourcePolicies first check and update current resource policies and then get the latest resource policies
func (mgr *ResPoliciesManager) getResourcePolicies(refName, refType string) (*ResourcePolicies, error) {
	// TODO currently we only want to support type of configmap
	if refType != "configmap" {
		return nil, fmt.Errorf("unsupported resource policies reference type %s", refType)
	}

	if mgr.resPolicies != nil {
		return mgr.resPolicies, nil
	}

	policiesConfigmap := &v1.ConfigMap{}
	err := mgr.kbClient.Get(context.Background(), kbClient.ObjectKey{Namespace: mgr.namespace, Name: refName}, policiesConfigmap)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get resource policies %s/%s configmap", refName, mgr.namespace)
	}

	res, err := GetResourcePoliciesFromConfig(policiesConfigmap)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get the user resource policies config")
	}
	if res == nil {
		return nil, fmt.Errorf("%s/%s resource policy not found", refName, refType)
	}

	mgr.resPolicies = res

	return mgr.resPolicies, nil
}

func (mgr *ResPoliciesManager) GetStructredVolumeFromPVC(item runtime.Unstructured) (*StructuredVolume, error) {
	pvc := v1.PersistentVolumeClaim{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &pvc); err != nil {
		return nil, errors.WithStack(err)
	}

	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		return nil, errors.Errorf("PVC has no volume backing this claim")
	}

	pv := &v1.PersistentVolume{}
	if err := mgr.kbClient.Get(context.Background(), kbClient.ObjectKey{Name: pvName}, pv); err != nil {
		return nil, errors.WithStack(err)
	}
	volume := StructuredVolume{}
	volume.ParsePV(pv)
	return &volume, nil
}

func (mgr *ResPoliciesManager) GetVolumeMatchedAction(refName, refType string, volume *StructuredVolume) (*Action, error) {
	resPolicies, err := mgr.getResourcePolicies(refName, refType)
	if err != nil {
		return nil, err
	}
	if resPolicies == nil {
		return nil, fmt.Errorf("failed to get resource policies %s/%s configmap", refName, mgr.namespace)
	}

	return getVolumeMatchedAction(resPolicies, volume), nil
}
