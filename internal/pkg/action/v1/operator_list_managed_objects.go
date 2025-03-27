package v1

import (
	"context"
	"errors"
	"fmt"
	"github.com/operator-framework/helm-operator-plugins/pkg/storage"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	helmstorage "helm.sh/helm/v3/pkg/storage"
	"io"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

type OperatorListManagedObjects struct {
	config *action.Configuration

	Package string

	Logf func(string, ...interface{})
}

func NewOperatorListManagedObjects(cfg *action.Configuration) *OperatorListManagedObjects {
	return &OperatorListManagedObjects{
		config: cfg,
		Logf:   func(string, ...interface{}) {},
	}
}

func (lmo *OperatorListManagedObjects) Run(ctx context.Context) ([]client.Object, error) {
	clusterExtension := &ocv1.ClusterExtension{}
	if err := lmo.config.Client.Get(ctx, types.NamespacedName{Name: lmo.Package}, clusterExtension); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &ErrPackageNotFound{lmo.Package}
		}
		return nil, fmt.Errorf("get clusterextension %q: %v", lmo.Package, err)
	}

	k8sClientset, err := kubernetes.NewForConfig(lmo.config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kubernetes clientset from config: %v", err)
	}
	d := storage.NewChunkedSecrets(k8sClientset.CoreV1().Secrets("olmv1-system"), "operator-controller", storage.ChunkedSecretsConfig{
		ChunkSize:      1024 * 1024,
		MaxReadChunks:  10,
		MaxWriteChunks: 10,
		Log:            func(format string, args ...interface{}) { lmo.Logf(format, args...) },
	})
	s := helmstorage.Init(d)
	rel, err := s.Deployed(lmo.Package)
	if err != nil {
		return nil, fmt.Errorf("failed to find deployed revision for cluster extension deployment: %v", err)
	}
	return manifestToObjects(strings.NewReader(rel.Manifest))
}

func manifestToObjects(manifestReader io.Reader) ([]client.Object, error) {
	var objs []client.Object
	dec := yaml.NewYAMLOrJSONDecoder(manifestReader, 1024)
	for {
		obj := unstructured.Unstructured{}
		err := dec.Decode(&obj)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		objs = append(objs, &obj)
	}
	return objs, nil
}
