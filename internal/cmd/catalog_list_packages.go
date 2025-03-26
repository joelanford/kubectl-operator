package cmd

import (
	"cmp"
	"encoding/json"
	"fmt"
	"github.com/Masterminds/semver/v3"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/kubectl-operator/internal/cmd/internal/log"
	internalaction "github.com/operator-framework/kubectl-operator/internal/pkg/action/v1"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"os"
	"sigs.k8s.io/yaml"
	"slices"
	"sync"
	"text/tabwriter"
)

func newCatalogListPackagesCmd(cfg *action.Configuration) *cobra.Command {
	cc := internalaction.NewCatalogContent(cfg)

	var (
		pkgName string
		output  string
	)

	cmd := &cobra.Command{
		Use:   "list-packages <catalog_name>",
		Short: "List packages from a cluster catalog",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cc.CatalogName = args[0]
			var (
				packages           = map[string]*packageMetadata{}
				bundleVersions     = map[string]semver.Version{}
				bundleDescriptions = map[string]string{}
				mu                 sync.Mutex
			)
			cc.WalkMetas = func(meta *declcfg.Meta, err error) error {
				if err != nil {
					return err
				}

				metaPkg := meta.Package
				if meta.Schema == declcfg.SchemaPackage {
					metaPkg = meta.Name
				}
				if pkgName != "" && pkgName != metaPkg {
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
							Name:                 pkg.Name,
							ChannelEntryNames:    make(map[string]sets.Set[string]),
							ChannelEntryVersions: make(map[string]sets.Set[semver.Version]),
						}
					}
					pkgMeta.Description = pkg.Description
					pkgMeta.DefaultChannel = pkg.DefaultChannel

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
							Name:                 ch.Package,
							ChannelEntryNames:    make(map[string]sets.Set[string]),
							ChannelEntryVersions: make(map[string]sets.Set[semver.Version]),
						}
					}
					chNames, ok := pkg.ChannelEntryNames[ch.Name]
					if !ok {
						chNames = sets.New[string]()
					}
					chVersions, ok := pkg.ChannelEntryVersions[ch.Name]
					if !ok {
						chVersions = sets.New[semver.Version]()
					}
					for _, entry := range ch.Entries {

						chNames.Insert(entry.Name)
						if bv, ok := bundleVersions[entry.Name]; ok {
							chVersions.Insert(bv)
						}

					}

					pkg.ChannelEntryNames[ch.Name] = chNames
					pkg.ChannelEntryVersions[ch.Name] = chVersions
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
							Name:                 bundle.Package,
							ChannelEntryNames:    make(map[string]sets.Set[string]),
							ChannelEntryVersions: make(map[string]sets.Set[semver.Version]),
						}
					}
					for chName, bundleNames := range pkg.ChannelEntryNames {
						if bundleNames.Has(bundle.Name) {
							pkg.ChannelEntryVersions[chName].Insert(*bundleVersion)
						}
					}
					packages[bundle.Package] = pkg
					return nil
				}
				return nil
			}

			if err := cc.Run(cmd.Context()); err != nil {
				log.Fatalf("failed to get content for catalog %q: %v", cc.CatalogName, err)
			}

			if len(packages) == 0 {
				log.Print("No resources found")
				return
			}

			var res result
			for _, pkg := range packages {
				pkgRes := packageResult{
					Name:           pkg.Name,
					Description:    pkg.Description,
					DefaultChannel: pkg.DefaultChannel,
				}
				for chName, entryNames := range pkg.ChannelEntryNames {
					entries := make([]entryResult, 0, len(entryNames))
					for name := range entryNames {
						entries = append(entries, entryResult{Name: name, Version: bundleVersions[name]})
					}
					slices.SortFunc(entries, func(a, b entryResult) int {
						return b.Version.Compare(&a.Version)
					})

					pkgRes.Channels = append(pkgRes.Channels, channelResult{
						Name:    chName,
						Entries: entries,
					})
				}
				slices.SortFunc(pkgRes.Channels, func(a, b channelResult) int {
					if pkg.DefaultChannel == a.Name {
						return -1
					}
					if pkg.DefaultChannel == b.Name {
						return 1
					}
					return cmp.Compare(a.Name, b.Name)
				})
				if pkgRes.Description == "" {
					defaultChannelHeadName := pkgRes.Channels[0].Entries[0].Name
					pkgRes.Description = bundleDescriptions[defaultChannelHeadName]
				}
				res.Packages = append(res.Packages, pkgRes)
			}
			slices.SortFunc(res.Packages, func(a, b packageResult) int {
				return cmp.Compare(a.Name, b.Name)
			})

			switch output {
			case "":
				tw := tabwriter.NewWriter(os.Stdout, 3, 4, 2, ' ', 0)
				_, _ = fmt.Fprintf(tw, "PACKAGE\t\tCHANNEL\tMAX VERSION\n")
				for _, pkg := range res.Packages {
					for _, ch := range pkg.Channels {
						defaultMarker := ""
						if ch.Name == pkg.DefaultChannel {
							defaultMarker = "*"
						}
						_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", pkg.Name, defaultMarker, ch.Name, ch.Entries[0].Version)
					}
				}
				_ = tw.Flush()
			case "json":
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(res); err != nil {
					log.Fatal(err)
				}
			case "yaml":
				yamlData, err := yaml.Marshal(res)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(string(yamlData))
			default:
				log.Fatalf("unknown output format: %q", output)
			}
		},
	}
	cmd.Flags().StringVarP(&pkgName, "package", "p", "", "package name to filter")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output format. One of: (json, yaml)")
	return cmd
}

type result struct {
	Packages []packageResult `json:"packages"`
}

type packageResult struct {
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	DefaultChannel string          `json:"defaultChannel"`
	Channels       []channelResult `json:"channels"`
}

type channelResult struct {
	Name    string        `json:"name"`
	Entries []entryResult `json:"entries"`
}

type entryResult struct {
	Name    string         `json:"name"`
	Version semver.Version `json:"version"`
}

type packageMetadata struct {
	Name                 string
	DefaultChannel       string
	Description          string
	ChannelEntryVersions map[string]sets.Set[semver.Version]
	ChannelEntryNames    map[string]sets.Set[string]
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
