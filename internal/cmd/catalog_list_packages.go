package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/Masterminds/semver/v3"
	"github.com/operator-framework/kubectl-operator/internal/cmd/internal/log"
	internalaction "github.com/operator-framework/kubectl-operator/internal/pkg/action/v1"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	"github.com/spf13/cobra"
	"os"
	"sigs.k8s.io/yaml"
	"text/tabwriter"
)

func newCatalogListPackagesCmd(cfg *action.Configuration) *cobra.Command {
	lp := internalaction.NewCatalogListPackages(cfg)

	var (
		pkgName string
		output  string
	)

	cmd := &cobra.Command{
		Use:   "list-packages <catalog_name>",
		Short: "List packages from a cluster catalog",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			lp.CatalogName = args[0]

			packages, err := lp.Run(cmd.Context())
			if err != nil {
				log.Fatalf("failed to get content for catalog %q: %v", lp.CatalogName, err)
			}

			if len(packages) == 0 {
				log.Print("No resources found")
				return
			}

			switch output {
			case "":
				tw := tabwriter.NewWriter(os.Stdout, 3, 4, 2, ' ', 0)
				_, _ = fmt.Fprintf(tw, "PACKAGE\t\tCHANNEL\tMAX VERSION\n")
				for _, pkg := range packages {
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
				if err := enc.Encode(packages); err != nil {
					log.Fatal(err)
				}
			case "yaml":
				yamlData, err := yaml.Marshal(packages)
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
