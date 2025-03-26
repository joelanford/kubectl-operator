package v1

import (
	"context"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
)

type CatalogRemove struct {
	config *action.Configuration

	CatalogName string
}

func NewCatalogRemove(cfg *action.Configuration) *CatalogRemove {
	return &CatalogRemove{
		config: cfg,
	}
}

func (r *CatalogRemove) Run(ctx context.Context) error {
	clusterCatalog := ocv1.ClusterCatalog{}
	clusterCatalog.SetName(r.CatalogName)
	return deleteAndWait(ctx, r.config.Client, &clusterCatalog)
}
