package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mcp "github.com/metoro-io/mcp-golang"
	mcpstdio "github.com/metoro-io/mcp-golang/transport/stdio"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"

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
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			server := mcp.NewServer(mcpstdio.NewStdioServerTransport())

			for _, tool := range []struct {
				name        string
				description string
				handler     any
			}{
				{
					name:        "list_cluster_catalogs",
					description: "List the ClusterCatalogs that are present in the cluster.",
					handler:     listClusterCatalogs(cfg),
				},
				{
					name:        "list_packages_from_catalog",
					description: "List the packages that are present in a given ClusterCatalog. This function returns a list of packages by name.",
					handler:     listPackagesFromCatalog(cfg),
				},
				{
					name:        "get_package_from_catalog",
					description: "Get the specified package metadata from the given ClusterCatalog. This function returns detailed metadata about a package. It is useful to call this after the packages have been listed in order to get more specific information about a particular package.",
					handler:     getPackageFromCatalog(cfg),
				},
				{
					name:        "create_cluster_catalog",
					description: "Add a ClusterCatalog to the cluster from which packages can be installed. The returned data includes the status of the catalog after creation.",
					handler:     createClusterCatalog(cfg),
				},
				{
					name:        "delete_cluster_catalog",
					description: "Delete a ClusterCatalog from the cluster so that its packages and bundles are no longer available for installation or upgrades",
					handler:     deleteClusterCatalog(cfg),
				},
				{
					name:        "list_cluster_extensions",
					description: "List the ClusterExtensions that are present in the cluster. These represent the set of packages that have actually been installed.",
					handler:     listClusterExtensions(cfg),
				},
				{
					name:        "install_cluster_extension",
					description: "Install a ClusterExtension in the cluster for a package at a particular version.",
					handler:     installClusterExtension(cfg),
				},
				{
					name:        "delete_cluster_extension",
					description: "Delete a ClusterExtension from the cluster. Beware, when a cluster extension is deleted, all of its managed objects are deleted. This can have cascading effects and cause data loss for users. Therefore, use of this function should be gated by asking the user to review the request parameters and warning them of the possible effects BEFORE actually calling it.",
					handler:     deleteClusterExtension(cfg),
				},
				{
					name:        "list_objects_managed_by_cluster_extension",
					description: "List all of the objects that are being managed under the umbrella of a ClusterExtension.",
					handler:     listClusterExtensionManagedObjects(cfg),
				},
				{
					name:        "get_example_custom_resources_for_cluster_extension",
					description: "This function requires the given cluster extension to already be installed. It gets the example custom resources provided by the extension author for the bundle currently installed for the given cluster extension (by its metadata.name). The returned objects can be applied to the cluster to actual instantiate services provided by the extension.",
					handler:     listExamplesFromCatalogForClusterExtension(cfg),
				},
				{
					name:        "apply_example_custom_resource",
					description: "Using examples provided by the 'get_example_custom_resources_for_cluster_extension' function, apply one of those examples on the cluster.",
					handler:     applyExampleCustomResource(cfg),
				},
				{
					name:        "get_arbitrary_object",
					description: "Get an arbitrary object from the cluster by its name, namespace, apiVersion, and kind. Namespace SHOULD be explicitly set to empty string for cluster-scoped objects.",
					handler:     getArbitraryObject(cfg),
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
			<-ctx.Done()
			fmt.Fprintln(os.Stderr, "MCP server stopped")
		},
	}

	return cmd
}

type listClusterCatalogsRequest struct{}
type listClusterExtensionsRequest struct{}

type listPackagesFromCatalogRequest struct {
	ClusterCatalogName string `json:"clusterCatalogName" jsonschema:"required,description=The name of the cluster catalog from which to list packages."`
}
type getPackageFromCatalogRequest struct {
	ClusterCatalogName string `json:"clusterCatalogName" jsonschema:"required,description=The name of the cluster catalog from which to get the package metadata."`
	PackageName        string `json:"packageName" jsonschema:"required,description=The name of the package to get metadata about."`
}

type createClusterCatalogRequest struct {
	CatalogImageReference string `json:"catalogImageReference" jsonschema:"required,description=The docker image reference of the OLM ClusterCatalog to create"`
	Name                  string `json:"name" jsonschema:"required,description=The name of the ClusterCatalog to create"`
	PollIntervalMinutes   *int   `json:"pollIntervalMinutes" jsonschema:"description=The polling interval to check for updates to the catalog. By default polling will be configured for 10 minute intervals. Set to 0 to disable polling."`
	Priority              int32  `json:"priority" jsonschema:"description=The priority of the Cluster Catalog to install. Default is 0, which is the mid-point of the range of allowed priorities. Catalogs with larger numbers have higher priority than catalogs with smaller numbers."`
}

type deleteClusterCatalogRequest struct {
	Name string `json:"name" jsonschema:"required,description=The metadata.name of the ClusterCatalog to delete"`
}

type createClusterExtensionRequest struct {
	PackageName string   `json:"packageName" jsonschema:"required,description=The name of the package to install"`
	Version     string   `json:"version" jsonschema:"description=The version (or version range, based on Masterminds) of the package to install. If not provided, OLM will install the latest available version and keep it updated automatically."`
	Channels    []string `json:"channels" jsonschema:"description=An optional list of channel names to use when installing and upgrading versions of this package. Users often want to set a channel based on their risk tolerance."`

	CatalogName string `json:"catalogName" jsonschema:"description=The name of the ClusterCatalog from which to source the package for installation. By default, OLM will install from the highest priority ClusterCatalog that contains the package. This is generally only necessary to use when multiple catalogs contain the same package name."`
}

type deleteClusterExtensionRequest struct {
	Name string `json:"name" jsonschema:"required,description=The metadata.name of the ClusterExtension to delete"`
}

type listClusterExtensionManagedObjectsRequest struct {
	ClusterExtensionName string `json:"clusterExtensionName" jsonschema:"required,description=The name of the ClusterExtension to list managed objects from"`
}

type getSamplesForClusterExtensionRequest struct {
	ClusterExtensionName string `json:"clusterExtensionName" jsonschema:"required,description=The name of the ClusterExtension to get samples for. The cluster extension name is required."`
	CatalogName          string `json:"catalogName" jsonschema:"requires,description=The name of the ClusterCatalog from which to lookup the samples. The catalog name is required."`
}

type applySamplesCustomResourceRequest struct {
	CustomResource string `json:"customResource" jsonschema:"required,description=The YAML or JSON of custom resource sample to apply to the cluster"`
}

type getObjectRequest struct {
	Name       string `json:"name" jsonschema:"required,description=The name of the object to get"`
	Namespace  string `json:"namespace" jsonschema:"required,description=The namespace of the object to get. If the object is cluster-scoped, this field should be set to an empty string."`
	APIVersion string `json:"apiVersion" jsonschema:"required,description=The API version of the object to get"`
	Kind       string `json:"kind" jsonschema:"required,description=The kind of the object to get"`
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

func listPackagesFromCatalog(cfg *action.Configuration) func(context.Context, listPackagesFromCatalogRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req listPackagesFromCatalogRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintln(os.Stderr, "listing packages from cluster catalog")
		listPackages := v1action.NewCatalogListPackages(cfg)
		listPackages.CatalogName = req.ClusterCatalogName

		packages, err := listPackages.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}
		for i := range packages {
			packages[i] = v1action.PackageSummary{Name: packages[i].Name}
		}
		return mcpToolResponse(packages)
	}
}

func getPackageFromCatalog(cfg *action.Configuration) func(context.Context, getPackageFromCatalogRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req getPackageFromCatalogRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintln(os.Stderr, "getting package from cluster catalog")
		listPackages := v1action.NewCatalogListPackages(cfg)
		listPackages.CatalogName = req.ClusterCatalogName
		listPackages.PackageName = req.PackageName

		packages, err := listPackages.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}

		if len(packages) == 0 {
			return mcpToolResponse(fmt.Errorf("no packages found in catalog %q with name %q", req.ClusterCatalogName, req.PackageName))
		}
		if len(packages) > 1 {
			return mcpToolResponse(fmt.Errorf("multiple packages found in catalog %q with name %q", req.ClusterCatalogName, req.PackageName))
		}
		if packages[0].Name != req.PackageName {
			return mcpToolResponse(fmt.Errorf("found package name %q that does not match expected package name %q", packages[0].Name, req.PackageName))
		}
		return mcpToolResponse(packages[0])
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

func deleteClusterCatalog(cfg *action.Configuration) func(context.Context, deleteClusterCatalogRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req deleteClusterCatalogRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintf(os.Stderr, "deleting cluster catalog from request %#v\n", req)
		deleteCatalog := v1action.NewCatalogRemove(cfg)
		deleteCatalog.CatalogName = req.Name

		if err := deleteCatalog.Run(ctx); err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(fmt.Sprintf("Successfully deleted cluster catalog %q", req.Name))
	}
}

func installClusterExtension(cfg *action.Configuration) func(context.Context, createClusterExtensionRequest) (*mcp.ToolResponse, error) {
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

func deleteClusterExtension(cfg *action.Configuration) func(context.Context, deleteClusterExtensionRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req deleteClusterExtensionRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintf(os.Stderr, "deleting cluster extension from request %#v\n", req)
		deleteExtension := v1action.NewOperatorUninstall(cfg)
		deleteExtension.Package = req.Name

		if err := deleteExtension.Run(ctx); err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(fmt.Sprintf("Successfully deleted cluster extension %q", req.Name))
	}
}

func listClusterExtensionManagedObjects(cfg *action.Configuration) func(context.Context, listClusterExtensionManagedObjectsRequest) (*mcp.ToolResponse, error) {
	type objectSummary struct {
		Name       string
		Namespace  string
		APIVersion string
		Kind       string
	}

	return func(ctx context.Context, req listClusterExtensionManagedObjectsRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintf(os.Stderr, "listing cluster extension managed objects from request %#v\n", req)
		listManagedObjects := v1action.NewOperatorListManagedObjects(cfg)
		listManagedObjects.Package = req.ClusterExtensionName
		managedObjects, err := listManagedObjects.Run(ctx)
		if err != nil {
			return mcpToolResponse(err)
		}

		objectSummaries := make([]objectSummary, 0, len(managedObjects))
		for _, managedObject := range managedObjects {
			objectSummaries = append(objectSummaries, objectSummary{
				Name:       managedObject.GetName(),
				Namespace:  managedObject.GetNamespace(),
				APIVersion: managedObject.GetObjectKind().GroupVersionKind().GroupVersion().String(),
				Kind:       managedObject.GetObjectKind().GroupVersionKind().Kind,
			})
		}
		return mcpToolResponse(objectSummaries)
	}
}

func listExamplesFromCatalogForClusterExtension(cfg *action.Configuration) func(context.Context, getSamplesForClusterExtensionRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req getSamplesForClusterExtensionRequest) (*mcp.ToolResponse, error) {
		fmt.Fprintf(os.Stderr, "getting samples from catalog for cluster extension %q\n", req.ClusterExtensionName)
		getSamples := v1action.NewCatalogContent(cfg)
		getSamples.CatalogName = req.CatalogName

		ce := &ocv1.ClusterExtension{}
		ce.SetName(req.ClusterExtensionName)
		if err := cfg.Client.Get(ctx, client.ObjectKeyFromObject(ce), ce); err != nil {
			return mcpToolResponse(err)
		}

		var bundleName string
		if ce.Status.Install != nil {
			bundleName = ce.Status.Install.Bundle.Name
		}
		if bundleName == "" {
			return mcpToolResponse(fmt.Errorf("no installed bundle found for cluster extension %q", req.ClusterExtensionName))
		}

		var (
			samples    []client.Object
			samplesErr error
		)
		getSamples.WalkMetas = func(meta *declcfg.Meta, err error) error {
			if err != nil {
				return err
			}
			if meta.Schema != declcfg.SchemaBundle {
				return nil
			}
			if meta.Name != bundleName {
				return nil
			}
			if len(samples) > 0 || samplesErr != nil {
				return nil
			}
			var b declcfg.Bundle
			if err := json.Unmarshal(meta.Blob, &b); err != nil {
				return err
			}
			samples, samplesErr = getBundleSamples(b)
			return nil
		}
		if err := getSamples.Run(ctx); err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(samples)
	}
}

func applyExampleCustomResource(cfg *action.Configuration) func(context.Context, applySamplesCustomResourceRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req applySamplesCustomResourceRequest) (*mcp.ToolResponse, error) {
		objs, err := manifestToObjects(req.CustomResource)
		if err != nil {
			return mcpToolResponse(err)
		}

		var (
			appliedObjects []client.Object
			applyErrors    []error
		)
		for _, obj := range objs {
			if err := cfg.Client.Patch(ctx, obj, client.Apply, client.ForceOwnership, client.FieldOwner("kubectl-operator-mcp")); err != nil {
				applyErrors = append(applyErrors, err)
			} else {
				appliedObjects = append(appliedObjects, obj)
			}
		}
		if len(applyErrors) > 0 {
			return mcpToolResponse(errors.Join(applyErrors...))
		}
		return mcpToolResponse(appliedObjects)
	}
}

func getArbitraryObject(cfg *action.Configuration) func(context.Context, getObjectRequest) (*mcp.ToolResponse, error) {
	return func(ctx context.Context, req getObjectRequest) (*mcp.ToolResponse, error) {
		obj := &unstructured.Unstructured{}
		obj.SetNamespace(req.Namespace)
		obj.SetName(req.Name)
		obj.SetAPIVersion(req.APIVersion)
		obj.SetKind(req.Kind)

		if err := cfg.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return mcpToolResponse(err)
		}
		return mcpToolResponse(obj)
	}
}

func getBundleSamples(b declcfg.Bundle) ([]client.Object, error) {
	for _, p := range b.Properties {
		switch p.Type {
		case property.TypeCSVMetadata:
			var v property.CSVMetadata
			if err := json.Unmarshal(p.Value, &v); err != nil {
				return nil, err
			}
			return manifestToObjects(v.Annotations["alm-examples"])
		case property.TypeBundleObject:
			var v property.BundleObject
			if err := json.Unmarshal(p.Value, &v); err != nil {
				return nil, err
			}
			var pm metav1.PartialObjectMetadata
			if err := json.Unmarshal(v.Data, &pm); err != nil {
				return nil, err
			}
			if pm.Kind != "ClusterServiceVersion" {
				continue
			}
			var csv v1alpha1.ClusterServiceVersion
			if err := json.Unmarshal(v.Data, &csv); err != nil {
				return nil, err
			}
			return manifestToObjects(csv.Annotations["alm-examples"])
		default:
			continue
		}
	}
	return nil, nil
}

func manifestToObjects(manifest string) ([]client.Object, error) {
	manifest = strings.TrimSpace(manifest)
	if strings.HasPrefix(manifest, "[") {
		var items []json.RawMessage
		if err := json.Unmarshal([]byte(manifest), &items); err != nil {
			return nil, err
		}
		manifest = ""
		for _, item := range items {
			manifest += string(item)
		}
	}

	var objs []client.Object
	dec := yaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 1024)
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
