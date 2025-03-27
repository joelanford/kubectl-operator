package cmd

import (
	"fmt"
	"github.com/Masterminds/semver/v3"
	"github.com/operator-framework/kubectl-operator/internal/cmd/internal/log"
	internalaction "github.com/operator-framework/kubectl-operator/internal/pkg/action/v1"
	"github.com/operator-framework/kubectl-operator/pkg/action"
	"github.com/spf13/cobra"
	"iter"
	"os"
	"strings"
	"text/tabwriter"
)

func newCatalogListBundlesCmd(cfg *action.Configuration) *cobra.Command {
	lb := internalaction.NewCatalogListBundles(cfg)

	var versionRange string

	cmd := &cobra.Command{
		Use:   "list-bundles <catalog_name>",
		Short: "List bundles from a cluster catalog",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			lb.CatalogName = args[0]
			constraints, err := getVersionRangeConstraints(versionRange)
			if err != nil {
				log.Fatal(err)
			}
			lb.VersionRange = *constraints

			bundles, err := lb.Run(cmd.Context())
			if err != nil {
				log.Fatal(err)
			}

			if len(bundles) == 0 {
				log.Print("No resources found")
				return
			}

			tw := tabwriter.NewWriter(os.Stdout, 3, 4, 2, ' ', 0)
			_, _ = fmt.Fprintf(tw, "PACKAGE\tNAME\tVERSION\tCHANNELS\t\n")

			for _, b := range bundles {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", b.Package, b.Name, b.Version, strings.Join(b.Channels, ","))

			}
			_ = tw.Flush()
		},
	}
	cmd.Flags().StringVarP(&lb.PackageName, "package", "p", "", "package name to filter")
	cmd.Flags().StringVarP(&versionRange, "version", "v", "", "version range to filter")
	cmd.Flags().StringSliceVarP(&lb.Channels, "channels", "c", []string{}, "channels to filter")
	return cmd
}

func getVersionRangeConstraints(versionRangeStr string) (*semver.Constraints, error) {
	if versionRangeStr != "" {
		return semver.NewConstraint(versionRangeStr)
	}
	all, err := semver.NewConstraint(">=0.0.0-0")
	if err != nil {
		panic("programmer invalid version range for matching all versions")
	}
	return all, nil
}

func collect[V any](i iter.Seq[V]) []V {
	var out []V
	for v := range i {
		out = append(out, v)
	}
	return out
}
