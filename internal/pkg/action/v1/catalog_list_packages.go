package v1

import (
	"cmp"
	"context"
	"encoding/json"
	"github.com/Masterminds/semver/v3"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"slices"
	"sync"
)

type CatalogListPackages struct {
	config *action.Configuration

	CatalogName string
	PackageName string

	Logf func(string, ...interface{})
}

type PackageSummary struct {
	Name           string           `json:"name"`
	Description    string           `json:"description,omitempty"`
	DefaultChannel string           `json:"defaultChannel,omitempty"`
	Channels       []ChannelSummary `json:"channels,omitempty"`
}

type ChannelSummary struct {
	Name    string                `json:"name"`
	Entries []ChannelEntrySummary `json:"entries,omitempty"`
}

type ChannelEntrySummary struct {
	Name    string         `json:"name,omitempty"`
	Version semver.Version `json:"version,omitempty"`
}

func NewCatalogListPackages(cfg *action.Configuration) *CatalogListPackages {
	return &CatalogListPackages{
		config: cfg,
		Logf:   func(string, ...interface{}) {},
	}
}

func (lb *CatalogListPackages) Run(ctx context.Context) ([]PackageSummary, error) {
	var (
		packages           = map[string]*packageMetadata{}
		bundleVersions     = map[string]semver.Version{}
		bundleDescriptions = map[string]string{}
		mu                 sync.Mutex
	)
	cc := CatalogContent{
		config:      lb.config,
		CatalogName: lb.CatalogName,
		WalkMetas: func(meta *declcfg.Meta, err error) error {
			if err != nil {
				return err
			}

			metaPkg := meta.Package
			if meta.Schema == declcfg.SchemaPackage {
				metaPkg = meta.Name
			}
			if lb.PackageName != "" && lb.PackageName != metaPkg {
				return nil
			}

			if meta.Schema == declcfg.SchemaPackage {
				var pkg declcfg.Package
				if err := json.Unmarshal(meta.Blob, &pkg); err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()

				pkgMeta, ok := packages[pkg.Name]
				if !ok {
					pkgMeta = &packageMetadata{
						name:                 pkg.Name,
						channelEntryNames:    make(map[string]sets.Set[string]),
						channelEntryVersions: make(map[string]sets.Set[semver.Version]),
					}
				}
				pkgMeta.description = pkg.Description
				pkgMeta.defaultChannel = pkg.DefaultChannel

				packages[pkg.Name] = pkgMeta
				return nil
			}
			if meta.Schema == declcfg.SchemaChannel {
				var ch declcfg.Channel
				if err := json.Unmarshal(meta.Blob, &ch); err != nil {
					return err
				}

				mu.Lock()
				defer mu.Unlock()

				pkg, ok := packages[ch.Package]
				if !ok {
					pkg = &packageMetadata{
						name:                 ch.Package,
						channelEntryNames:    make(map[string]sets.Set[string]),
						channelEntryVersions: make(map[string]sets.Set[semver.Version]),
					}
				}
				chNames, ok := pkg.channelEntryNames[ch.Name]
				if !ok {
					chNames = sets.New[string]()
				}
				chVersions, ok := pkg.channelEntryVersions[ch.Name]
				if !ok {
					chVersions = sets.New[semver.Version]()
				}
				for _, entry := range ch.Entries {

					chNames.Insert(entry.Name)
					if bv, ok := bundleVersions[entry.Name]; ok {
						chVersions.Insert(bv)
					}

				}

				pkg.channelEntryNames[ch.Name] = chNames
				pkg.channelEntryVersions[ch.Name] = chVersions
				packages[ch.Package] = pkg
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
				bundleDescription, err := getBundleDescription(bundle)
				if err != nil {
					return err
				}

				mu.Lock()
				defer mu.Unlock()

				bundleVersions[bundle.Name] = *bundleVersion
				bundleDescriptions[bundle.Name] = bundleDescription

				pkg, ok := packages[bundle.Package]
				if !ok {
					pkg = &packageMetadata{
						name:                 bundle.Package,
						channelEntryNames:    make(map[string]sets.Set[string]),
						channelEntryVersions: make(map[string]sets.Set[semver.Version]),
					}
				}
				for chName, bundleNames := range pkg.channelEntryNames {
					if bundleNames.Has(bundle.Name) {
						pkg.channelEntryVersions[chName].Insert(*bundleVersion)
					}
				}
				packages[bundle.Package] = pkg
				return nil
			}
			return nil
		},
	}

	if err := cc.Run(ctx); err != nil {
		log.Fatalf("failed to get content for catalog %q: %v", cc.CatalogName, err)
	}

	var packageResults []PackageSummary
	for _, pkg := range packages {
		pkgRes := PackageSummary{
			Name:           pkg.name,
			Description:    pkg.description,
			DefaultChannel: pkg.defaultChannel,
		}
		for chName, entryNames := range pkg.channelEntryNames {
			entries := make([]ChannelEntrySummary, 0, len(entryNames))
			for name := range entryNames {
				entries = append(entries, ChannelEntrySummary{Name: name, Version: bundleVersions[name]})
			}
			slices.SortFunc(entries, func(a, b ChannelEntrySummary) int {
				return b.Version.Compare(&a.Version)
			})

			pkgRes.Channels = append(pkgRes.Channels, ChannelSummary{
				Name:    chName,
				Entries: entries,
			})
		}
		slices.SortFunc(pkgRes.Channels, func(a, b ChannelSummary) int {
			if pkg.defaultChannel == a.Name {
				return -1
			}
			if pkg.defaultChannel == b.Name {
				return 1
			}
			return cmp.Compare(a.Name, b.Name)
		})
		if pkgRes.Description == "" {
			defaultChannelHeadName := pkgRes.Channels[0].Entries[0].Name
			pkgRes.Description = bundleDescriptions[defaultChannelHeadName]
		}
		packageResults = append(packageResults, pkgRes)
	}
	slices.SortFunc(packageResults, func(a, b PackageSummary) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return packageResults, nil
}

type packageMetadata struct {
	name                 string
	defaultChannel       string
	description          string
	channelEntryVersions map[string]sets.Set[semver.Version]
	channelEntryNames    map[string]sets.Set[string]
}

func getBundleDescription(b declcfg.Bundle) (string, error) {
	for _, p := range b.Properties {
		switch p.Type {
		case property.TypeCSVMetadata:
			var v property.CSVMetadata
			if err := json.Unmarshal(p.Value, &v); err != nil {
				return "", err
			}
			return v.Description, nil
		case property.TypeBundleObject:
			var v property.BundleObject
			if err := json.Unmarshal(p.Value, &v); err != nil {
				return "", err
			}
			var pm metav1.PartialObjectMetadata
			if err := json.Unmarshal(v.Data, &pm); err != nil {
				return "", err
			}
			if pm.Kind != "ClusterServiceVersion" {
				continue
			}
			var csv v1alpha1.ClusterServiceVersion
			if err := json.Unmarshal(v.Data, &csv); err != nil {
				return "", err
			}
			return csv.Spec.Description, nil
		default:
			continue
		}
	}
	return "", nil
}
