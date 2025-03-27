package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"os/signal"
	"syscall"
	"time"

	mcp "github.com/metoro-io/mcp-golang"
	mcpstdio "github.com/metoro-io/mcp-golang/transport/stdio"

	"github.com/operator-framework/kubectl-operator/internal/cmd/internal/log"
	v1action "github.com/operator-framework/kubectl-operator/internal/pkg/action/v1"
	"github.com/operator-framework/kubectl-operator/pkg/action"
)

func newMCPCmd(cfg *action.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Run the model context protocol server",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			sigIntCtx, sigIntCancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
			sigTermCtx, sigTermCancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
			defer sigIntCancel()
			defer sigTermCancel()

			server := mcp.NewServer(mcpstdio.NewStdioServerTransport())

			for _, tool := range []struct {
				name        string
				description string
				handler     any
			}{
				{
					name:        "ListClusterCatalogs",
					description: "List the ClusterCatalogs that are present in the cluster.",
					handler:     listClusterCatalogs(cfg),
				},
				{
					name:        "GetOrListPackagesFromCatalog",
					description: "Get or list the packages that are present in a given ClusterCatalog.",
					handler:     getOrListPackagesFromCatalog(cfg),
				},
				{
					name:        "CreateClusterCatalog",
					description: "Create a ClusterCatalog in the cluster from which packages can be installed. The returned data includes the status of the catalog after creation.",
					handler:     createClusterCatalog(cfg),
				},
				{
					name:        "ListClusterExtensions",
					description: "List the ClusterExtensions that are present in the cluster.",
					handler:     listClusterExtensions(cfg),
				},
				{
					name:        "CreateClusterExtension",
					description: "Creates a ClusterExtension in the cluster to install a package at a particular version.",
					handler:     createClusterExtension(cfg),
				},
			} {
				if err := server.RegisterTool(tool.name, tool.description, tool.handler); err != nil {
					log.Fatal(err)
				}
			}
			if err := server.Serve(); err != nil {
				log.Fatal(err)
			}
			fmt.Fprintln(os.Stderr, "MCP server started")
			select {
			case <-sigIntCtx.Done():
				fmt.Fprintln(os.Stderr, "SIGINT: MCP server stopped")
			case <-sigTermCtx.Done():
				fmt.Fprintln(os.Stderr, "SIGTERM: MCP server stopped")
			}
		},
	}

	return cmd
}

type listClusterCatalogsRequest struct{}
type listClusterExtensionsRequest struct{}

type getOrListPackagesFromCatalogsRequest struct {
	ClusterCatalogName string `json:"clusterCatalogName" jsonschema:"required,description=The name of the cluster catalog from which to list packages."`
	PackageName        string `json:"packageName" jsonschema:"description=Optionally, a specific package name to list. When a package name is provided, the response will contain much more detailed information about the particular package."`
}

type createClusterCatalogRequest struct {
	CatalogImageReference string `json:"catalogImageReference" jsonschema:"required,description=The docker image reference of the OLM ClusterCatalog to create"`
	Name                  string `json:"name" jsonschema:"required,description=The name of the ClusterCatalog to create"`
	PollIntervalMinutes   *int   `json:"pollIntervalMinutes" jsonschema:"description=The polling interval to check for updates to the catalog. By default polling will be configured for 10 minute intervals. Set to 0 to disable polling."`
	Priority              int32  `json:"priority" jsonschema:"description=The priority of the Cluster Catalog to install. Default is 0, which is the mid-point of the range of allowed priorities. Catalogs with larger numbers have higher priority than catalogs with smaller numbers."`
}

type createClusterExtensionRequest struct {
	PackageName string   `json:"packageName" jsonschema:"required,description=The name of the package to install"`
	Version     string   `json:"version" jsonschema:"description=The version (or version range, based on Masterminds) of the package to install. If not provided, OLM will install the latest available version and keep it updated automatically."`
	Channels    []string `json:"channels" jsonschema:"description=An optional list of channel names to use when installing and upgrading versions of this package. Users often want to set a channel based on their risk tolerance."`

	CatalogName string `json:"catalogName" jsonschema:"description=The name of the ClusterCatalog from which to source the package for installation. By default, OLM will install from the highest priority ClusterCatalog that contains the package. This is generally only necessary to use when multiple catalogs contain the same package name."`
}

func mcpToolResponse(v any) (*mcp.ToolResponse, error) {
	responseText := ""
	if err, ok := v.(error); ok {
		responseText = fmt.Sprintf("error: %v", err.Error())
	} else if jsonData, err := json.Marshal(v); err != nil {
		responseText = fmt.Sprintf("error: %v", err.Error())
	} else {
		responseText = string(jsonData)
	}
	fmt.Fprintln(os.Stderr, "produced response:", responseText)
	return mcp.NewToolResponse(mcp.NewTextContent(responseText)), nil
}

func listClusterCatalogs(cfg *action.Configuration) func(context.Context, listClusterCatalogsRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, _ listClusterCatalogsRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintln(os.Stderr, "listing cluster catalogs")
		listCatalogs := v1action.NewCatalogList(cfg)
		clusterCatalogs, err := listCatalogs.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(clusterCatalogs)
	}
}

func getOrListPackagesFromCatalog(cfg *action.Configuration) func(context.Context, getOrListPackagesFromCatalogsRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req getOrListPackagesFromCatalogsRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintln(os.Stderr, "listing packages from cluster catalog")
		listPackages := v1action.NewCatalogListPackages(cfg)
		listPackages.CatalogName = req.ClusterCatalogName
		listPackages.PackageName = req.PackageName

		packages, err := listPackages.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}
		if req.PackageName == "" {
			for i := range packages {
				packages[i] = v1action.PackageSummary{Name: packages[i].Name}
			}
		}
		return mcpToolResponse(packages)
	}
}

func listClusterExtensions(cfg *action.Configuration) func(context.Context, listClusterExtensionsRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, _ listClusterExtensionsRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintln(os.Stderr, "listing cluster extensions")
		listExtensions := v1action.NewOperatorList(cfg)
		clusterExtensions, err := listExtensions.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(clusterExtensions)
	}
}

func createClusterCatalog(cfg *action.Configuration) func(context.Context, createClusterCatalogRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req createClusterCatalogRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintf(os.Stderr, "creating cluster catalog from request %#v\n", req)
		createCatalog := v1action.NewCatalogAdd(cfg)
		createCatalog.CatalogName = req.Name
		createCatalog.CatalogImage = req.CatalogImageReference
		createCatalog.PollIntervalMinutes = 10
		if req.PollIntervalMinutes != nil {
			createCatalog.PollIntervalMinutes = *req.PollIntervalMinutes
		}
		createCatalog.Priority = req.Priority

		catalog, err := createCatalog.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(catalog)
	}
}

func createClusterExtension(cfg *action.Configuration) func(context.Context, createClusterExtensionRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req createClusterExtensionRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintf(os.Stderr, "creating cluster extension from request %#v\n", req)

		createExtension := v1action.NewOperatorInstall(cfg)
		createExtension.Package = req.PackageName
		createExtension.Version = req.Version
		createExtension.Channels = req.Channels
		createExtension.Namespace.Name = cfg.Namespace
		createExtension.ServiceAccount = fmt.Sprintf("%s-installer", req.PackageName)
		createExtension.MissingRulesHandler = createExtension.AutoCreateMissingRules

		if req.CatalogName != "" {
			createExtension.CatalogSelector = metav1.LabelSelector{
				MatchLabels: map[string]string{ocv1.MetadataNameLabel: req.CatalogName},
			}
		}

		timeout := time.Second * 30
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ce, err := createExtension.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(ce)
	}
}
