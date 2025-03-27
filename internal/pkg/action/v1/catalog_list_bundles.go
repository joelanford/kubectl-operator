package v1

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Masterminds/semver/v3"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"slices"
	"sync"
)

type CatalogListBundles struct {
	config *action.Configuration

	CatalogName  string
	PackageName  string
	VersionRange semver.Constraints
	Channels     []string

	Logf func(string, ...interface{})
}

type CatalogBundle struct {
	Package  string         `json:"package"`
	Name     string         `json:"name"`
	Version  semver.Version `json:"version"`
	Channels []string       `json:"channels"`
}

type bundleMetadata struct {
	Name     string
	Version  *semver.Version
	Channels sets.Set[string]
}

func NewCatalogListBundles(cfg *action.Configuration) *CatalogListBundles {
	return &CatalogListBundles{
		config: cfg,
		Logf:   func(string, ...interface{}) {},
	}
}

func (lb *CatalogListBundles) Run(ctx context.Context) ([]CatalogBundle, error) {
	var (
		bundles = map[string]map[string]*bundleMetadata{}
		mu      sync.Mutex
	)
	cc := CatalogContent{
		config:      lb.config,
		CatalogName: lb.CatalogName,
		WalkMetas: func(meta *declcfg.Meta, err error) error {
			if err != nil {
				return err
			}
			if lb.PackageName != "" && lb.PackageName != meta.Package {
				return nil
			}
			if meta.Schema == declcfg.SchemaChannel {
				var ch declcfg.Channel
				if err := json.Unmarshal(meta.Blob, &ch); err != nil {
					return err
				}
				for _, entry := range ch.Entries {
					mu.Lock()
					pkg, ok := bundles[ch.Package]
					if !ok {
						pkg = map[string]*bundleMetadata{}
					}
					b, ok := pkg[entry.Name]
					if !ok {
						b = &bundleMetadata{
							Name:     entry.Name,
							Channels: sets.New[string](),
						}
						pkg[entry.Name] = b
					}
					b.Channels.Insert(meta.Name)
					bundles[ch.Package] = pkg
					mu.Unlock()
				}
				return nil
			}
			if meta.Schema == declcfg.SchemaBundle {
				bundle := declcfg.Bundle{}
				if err := json.Unmarshal(meta.Blob, &bundle); err != nil {
					return err
				}
				bundleVersion, err := getBundleVersion(bundle)
				if err != nil {
					return err
				}
				mu.Lock()
				pkg, ok := bundles[bundle.Package]
				if !ok {
					pkg = map[string]*bundleMetadata{}
				}
				b, ok := pkg[bundle.Name]
				if !ok {
					b = &bundleMetadata{
						Name:     meta.Name,
						Channels: sets.New[string](),
					}
					pkg[bundle.Name] = b
				}
				b.Version = bundleVersion
				bundles[bundle.Package] = pkg
				mu.Unlock()
			}
			return nil
		},
	}

	if err := cc.Run(ctx); err != nil {
		log.Fatalf("failed to get content for catalog %q: %v", cc.CatalogName, err)
	}

	var catalogBundles []CatalogBundle
	for pkg, bundleMetas := range bundles {
		for _, bundleMeta := range bundleMetas {
			cb := CatalogBundle{
				Package:  pkg,
				Name:     bundleMeta.Name,
				Version:  semver.Version{},
				Channels: sets.List(bundleMeta.Channels),
			}
			if bundleMeta.Version != nil {
				cb.Version = *bundleMeta.Version
			}
			catalogBundles = append(catalogBundles, cb)
		}
	}
	slices.SortFunc(catalogBundles, func(a, b CatalogBundle) int {
		if d := cmp.Compare(a.Package, b.Package); d != 0 {
			return d
		}
		return b.Version.Compare(&a.Version)
	})
	return catalogBundles, nil
}

func getBundleVersion(b declcfg.Bundle) (*semver.Version, error) {
	packageValue := json.RawMessage{}
	for _, p := range b.Properties {
		if p.Type == property.TypePackage {
			packageValue = p.Value
			break
		}
	}
	if len(packageValue) == 0 {
		return nil, fmt.Errorf("no package property found")
	}
	packageProp := property.Package{}
	if err := json.Unmarshal(packageValue, &packageProp); err != nil {
		return nil, err
	}
	return semver.NewVersion(packageProp.Version)
}
