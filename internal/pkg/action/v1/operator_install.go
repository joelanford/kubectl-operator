package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"k8s.io/apimachinery/pkg/types"
	"math/big"
	"time"

	"github.com/operator-framework/kubectl-operator/pkg/action"
	ocv1 "github.com/operator-framework/operator-controller/api/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	applyconfigurationscorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applyconfigurationsrbacv1 "k8s.io/client-go/applyconfigurations/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type OperatorInstall struct {
	config *action.Configuration

	Namespace      OperatorInstallNamespaceConfig
	ServiceAccount string

	Package             string
	Channels            []string
	Version             string
	CatalogSelector     metav1.LabelSelector
	MissingRulesHandler func(context.Context, *ocv1.ClusterExtension) (bool, error)
	CleanupTimeout      time.Duration

	Logf func(string, ...interface{})
}

type OperatorInstallNamespaceConfig struct {
	Name        string
	Labels      map[string]string
	Annotations map[string]string
}

func NewOperatorInstall(cfg *action.Configuration) *OperatorInstall {
	return &OperatorInstall{
		config: cfg,
		Logf:   func(string, ...interface{}) {},
	}
}

func (i *OperatorInstall) applyNamespace(ctx context.Context) error {
	ac := applyconfigurationscorev1.Namespace(i.Namespace.Name)
	if i.Namespace.Labels != nil {
		ac = ac.WithLabels(i.Namespace.Labels)
	}
	if i.Namespace.Annotations != nil {
		ac = ac.WithAnnotations(i.Namespace.Annotations)
	}
	return patchObject(ctx, i.config.Client, ac)
}

func (i *OperatorInstall) applyServiceAccount(ctx context.Context) error {
	ac := applyconfigurationscorev1.ServiceAccount(i.ServiceAccount, i.Namespace.Name)
	return patchObject(ctx, i.config.Client, ac)
}

func (i *OperatorInstall) applyClusterRole(ctx context.Context, key types.NamespacedName, labels map[string]string, rules []*applyconfigurationsrbacv1.PolicyRuleApplyConfiguration) error {
	ac := applyconfigurationsrbacv1.ClusterRole(key.Name).WithLabels(labels).WithRules(rules...)
	return patchObject(ctx, i.config.Client, ac)
}

func (i *OperatorInstall) applyRole(ctx context.Context, key types.NamespacedName, labels map[string]string, rules []*applyconfigurationsrbacv1.PolicyRuleApplyConfiguration) error {
	ac := applyconfigurationsrbacv1.Role(key.Name, key.Namespace).WithLabels(labels).WithRules(rules...)
	return patchObject(ctx, i.config.Client, ac)
}

func (i *OperatorInstall) applyClusterRoleBinding(ctx context.Context, key types.NamespacedName, labels map[string]string, subject *applyconfigurationsrbacv1.SubjectApplyConfiguration, roleRef *applyconfigurationsrbacv1.RoleRefApplyConfiguration) error {
	ac := applyconfigurationsrbacv1.ClusterRoleBinding(key.Name).WithLabels(labels).WithSubjects(subject).WithRoleRef(roleRef)
	return patchObject(ctx, i.config.Client, ac)
}

func (i *OperatorInstall) applyRoleBinding(ctx context.Context, key types.NamespacedName, labels map[string]string, subject *applyconfigurationsrbacv1.SubjectApplyConfiguration, roleRef *applyconfigurationsrbacv1.RoleRefApplyConfiguration) error {
	ac := applyconfigurationsrbacv1.RoleBinding(key.Name, key.Namespace).WithLabels(labels).WithSubjects(subject).WithRoleRef(roleRef)
	return patchObject(ctx, i.config.Client, ac)
}

func (i *OperatorInstall) applyClusterExtension(ctx context.Context) error {
	catalogSource := map[string]interface{}{
		"packageName": i.Package,
	}
	if i.Version != "" {
		catalogSource["version"] = i.Version
	}
	if i.Channels != nil {
		catalogSource["channels"] = i.Channels
	}
	if i.CatalogSelector.MatchLabels != nil || i.CatalogSelector.MatchExpressions != nil {
		catalogSource["selector"] = i.CatalogSelector
	}
	u := unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": ocv1.GroupVersion.String(),
		"kind":       "ClusterExtension",
		"metadata": map[string]interface{}{
			"name": i.Package,
		},
		"spec": map[string]interface{}{
			"namespace": i.Namespace.Name,
			"serviceAccount": map[string]interface{}{
				"name": i.ServiceAccount,
			},
			"source": map[string]interface{}{
				"sourceType": "Catalog",
				"catalog":    catalogSource,
			},
		},
	}}
	return patchObject(ctx, i.config.Client, &u)
}

func (i *OperatorInstall) Run(ctx context.Context) (*ocv1.ClusterExtension, error) {
	if err := i.applyNamespace(ctx); err != nil {
		return nil, fmt.Errorf("apply namespace %q: %v", i.Namespace.Name, err)
	}
	if err := i.applyServiceAccount(ctx); err != nil {
		return nil, fmt.Errorf("apply service account %q: %v", i.ServiceAccount, err)
	}

	if err := i.applyClusterExtension(ctx); err != nil {
		return nil, fmt.Errorf("apply cluster extension: %v", err)
	}

	clusterExtension, err := i.waitForClusterExtensionInstalled(ctx)
	if err != nil {
		err = fmt.Errorf("wait for cluster extension installation: %v", err)
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), i.CleanupTimeout)
		defer cancelCleanup()
		cleanupErr := i.cleanup(cleanupCtx)
		cleanupErr = fmt.Errorf("error cleaning up cluster extension installation: %w", err)
		return nil, errors.Join(err, cleanupErr)
	}
	return clusterExtension, nil
}

func (i *OperatorInstall) waitForClusterExtensionInstalled(ctx context.Context) (*ocv1.ClusterExtension, error) {
	clusterExtension := &ocv1.ClusterExtension{
		ObjectMeta: metav1.ObjectMeta{
			Name: i.Package,
		},
	}
	errMsg := ""
	key := client.ObjectKeyFromObject(clusterExtension)
	if err := wait.PollUntilContextCancel(ctx, time.Millisecond*250, true, func(conditionCtx context.Context) (bool, error) {
		if err := i.config.Client.Get(conditionCtx, key, clusterExtension); err != nil {
			return false, err
		}

		observedGeneration := int64(-1)
		for _, cond := range clusterExtension.Status.Conditions {
			observedGeneration = max(observedGeneration, cond.ObservedGeneration)
		}

		if observedGeneration != clusterExtension.Generation {
			return false, nil
		}

		if clusterExtension.Status.Rules != nil && len(clusterExtension.Status.Rules.Missing) > 0 && i.MissingRulesHandler != nil {
			done, err := i.MissingRulesHandler(ctx, clusterExtension)
			if err != nil || !done {
				return false, err
			}
		}

		progressingCondition := meta.FindStatusCondition(clusterExtension.Status.Conditions, ocv1.TypeProgressing)
		if progressingCondition != nil && progressingCondition.Reason != ocv1.ReasonSucceeded {
			errMsg = progressingCondition.Message
			return false, nil
		}
		if !meta.IsStatusConditionPresentAndEqual(clusterExtension.Status.Conditions, ocv1.TypeInstalled, metav1.ConditionTrue) {
			return false, nil
		}
		return true, nil
	}); err != nil {
		if errMsg == "" {
			errMsg = err.Error()
		}
		return nil, fmt.Errorf("cluster extension %q did not finish installing: %s", clusterExtension.Name, errMsg)
	}
	return clusterExtension, nil
}

func (i *OperatorInstall) cleanup(ctx context.Context) error {
	u := OperatorUninstall{
		config:  i.config,
		Package: i.Package,
		Logf:    i.Logf,
	}
	return u.Run(ctx)
}

func (i *OperatorInstall) AutoCreateMissingRules(ctx context.Context, clusterExtension *ocv1.ClusterExtension) (bool, error) {

	subject := rbacv1.Subject{
		Kind:      "ServiceAccount",
		Name:      i.ServiceAccount,
		Namespace: i.Namespace.Name,
	}

	if clusterExtension.Status.Rules == nil || len(clusterExtension.Status.Rules.Missing) == 0 {
		return true, nil
	}

	assumeDone := len(clusterExtension.Status.Rules.Missing) < 64
	for _, missingRuleItem := range clusterExtension.Status.Rules.Missing {
		assumeDone = assumeDone && len(missingRuleItem.Rules) < 1024

		if len(missingRuleItem.Rules) == 0 {
			continue
		}

		nameDigest, err := hashToString(missingRuleItem.Rules)
		if err != nil {
			return false, err
		}
		roleKey := types.NamespacedName{Namespace: missingRuleItem.Namespace, Name: fmt.Sprintf("olmv1:clusterextension:%s:%s", clusterExtension.Name, nameDigest)}
		bindingKey := types.NamespacedName{Namespace: missingRuleItem.Namespace, Name: fmt.Sprintf("olmv1:clusterextension:%s:sa:%s:%s", clusterExtension.Name, clusterExtension.Spec.ServiceAccount.Name, nameDigest)}

		applyRole := i.applyClusterRole
		applyBinding := i.applyClusterRoleBinding
		roleRef := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleKey.Name}
		roleResource := "clusterrole"
		bindingResource := "clusterrolebinding"
		if missingRuleItem.Namespace != "" {
			applyRole = i.applyRole
			applyBinding = i.applyRoleBinding
			roleRef.Kind = "Role"
			roleResource = "role"
			bindingResource = "rolebinding"
		}

		labels := map[string]string{
			"olm.operatorframework.io/kubectl-operator-autogenerated": "true",
			"olm.operatorframework.io/cluster-extension-name":         clusterExtension.Name,
		}

		var acRules []*applyconfigurationsrbacv1.PolicyRuleApplyConfiguration
		for _, rule := range missingRuleItem.Rules {
			acRules = append(acRules, applyconfigurationsrbacv1.PolicyRule().
				WithAPIGroups(rule.APIGroups...).
				WithResources(rule.Resources...).
				WithVerbs(rule.Verbs...).
				WithResourceNames(rule.ResourceNames...).
				WithNonResourceURLs(rule.NonResourceURLs...),
			)
		}

		if err := applyRole(ctx, roleKey, labels, acRules); err != nil {
			return false, fmt.Errorf("apply %s %q: %w", roleResource, roleKey, err)
		}
		if err := applyBinding(ctx, bindingKey, labels,
			applyconfigurationsrbacv1.Subject().
				WithAPIGroup(subject.APIGroup).
				WithKind(subject.Kind).
				WithNamespace(subject.Namespace).
				WithName(subject.Name),
			applyconfigurationsrbacv1.RoleRef().
				WithAPIGroup(roleRef.APIGroup).
				WithKind(roleRef.Kind).
				WithName(roleRef.Name),
		); err != nil {
			return false, fmt.Errorf("apply %s %q: %w", bindingResource, bindingKey, err)
		}

	}
	return assumeDone, nil
}

func hashToString(itemToHash any) (string, error) {
	digester := fnv.New64a()
	digestEncoder := json.NewEncoder(digester)
	if err := digestEncoder.Encode(itemToHash); err != nil {
		return "", err
	}

	var digest []byte
	digest = digester.Sum(digest)

	var i big.Int
	i.SetBytes(digest[:])
	return i.Text(36), nil
}
