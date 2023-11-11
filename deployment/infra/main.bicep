@description('Location for all resources')
param location string = resourceGroup().location

var prefix = 'ks'
var suffix = uniqueString(resourceGroup().id)

resource storage 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: '${prefix}storage${suffix}'
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
  }

  resource blobServices 'blobServices' = {
    name: 'default'

    resource devContainer 'containers' = {
      name: 'dev'
      properties: {
        publicAccess: 'None'
      }
    }

    resource testContainer 'containers' = {
      name: 'test'
      properties: {
        publicAccess: 'None'
      }
    }
  }
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' = {
  name: '${prefix}registry${suffix}'
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
  name: '${prefix}-containerFetchingId-${suffix}'
  location: location
}

//  Identity orchestrating, i.e. accessing Kusto + Storage
resource appIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-app-id-${suffix}'
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

resource appEnvironment 'Microsoft.App/managedEnvironments@2022-10-01' = {
  name: '${prefix}-app-env-${suffix}'
  location: location
  sku: {
    name: 'Consumption'
  }
  properties: {
    zoneRedundant: false
  }
}

resource app 'Microsoft.App/containerApps@2022-10-01' = {
  name: '${prefix}-app-${suffix}'
  location: location
  dependsOn: [
    userIdentityRbacAuthorization
  ]
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${containerFetchingIdentity.id}': {}
      '${appIdentity.id}': {}
    }
  }
  properties: {
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        allowInsecure: false
        exposedPort: 0
        external: false
        targetPort: 80
        transport: 'auto'
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
      registries: [
        {
          identity: containerFetchingIdentity.id
          server: registry.properties.loginServer
        }
      ]
      secrets: []
    }
    environmentId: appEnvironment.id
    template: {
      containers: [
        {
          image: '${registry.name}.azurecr.io/kusto/kusto-split:latest'
          name: 'worker'
          resources: {
            cpu: '0.25'
            memory: '0.5Gi'
          }
          env: [
            {
              name: 'SourceBlob'
              value: 'https://${storage.name}.blob.core.windows.net/dev/adx.gz'
            }
            {
              name: 'DestinationBlobPrefix'
              value: 'https://${storage.name}.blob.core.windows.net/dev/split'
            }
            {
              name: 'InputCompression'
              value: 'Gzip'
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}
