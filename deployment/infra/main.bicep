@description('Location for all resources')
param location string = resourceGroup().location

var suffix = uniqueString(resourceGroup().id)

resource registry 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' = {
  name: 'registry${suffix}'
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: true
    anonymousPullEnabled: false
    dataEndpointEnabled: false
    policies: {
      azureADAuthenticationAsArmPolicy: {
        status: 'enabled'
      }
      retentionPolicy: {
        status: 'disabled'
      }
      softDeletePolicy: {
        status: 'disabled'
      }
    }
    publicNetworkAccess: 'enabled'
    zoneRedundancy: 'disabled'
  }
}

resource containerFetchingIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: 'containerFetchingId-${suffix}'
  location: location
}

//  We also need to authorize the user identity to pull container images from the registry
resource userIdentityRbacAuthorization 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containerFetchingIdentity.id, registry.id, 'rbac')
  scope: registry

  properties: {
    description: 'Giving AcrPull RBAC to identity'
    principalId: containerFetchingIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
  }
}
