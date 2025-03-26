package v1

import (
	"context"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"

	"github.com/operator-framework/kubectl-operator/pkg/action"
)

type CatalogList struct {
	config *action.Configuration
}

func NewCatalogList(cfg *action.Configuration) *CatalogList {
	return &CatalogList{cfg}
}

func (l *CatalogList) Run(ctx context.Context) ([]ocv1.ClusterCatalog, error) {
	clusterCatalogList := ocv1.ClusterCatalogList{}
	if err := l.config.Client.List(ctx, &clusterCatalogList); err != nil {
		return nil, err
	}
	return clusterCatalogList.Items, nil
}
