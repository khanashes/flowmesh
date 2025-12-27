package client

import (
	"github.com/flowmesh/engine/api/proto/flowmeshpb"
)

// buildResourcePath builds a ResourcePath proto message from components
func buildResourcePath(tenant, namespace, resourceType, name string) *flowmeshpb.ResourcePath {
	return &flowmeshpb.ResourcePath{
		Tenant:       tenant,
		Namespace:    namespace,
		ResourceType: resourceType,
		Name:         name,
	}
}
