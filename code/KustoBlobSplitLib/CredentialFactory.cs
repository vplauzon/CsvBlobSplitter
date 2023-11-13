using Azure.Core;
using Azure.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib
{
    public static class CredentialFactory
    {
        public static TokenCredential GetCredentials(RunSettings runSettings)
        {
            switch (runSettings.AuthMode)
            {
                case AuthMode.Default:
                    return new DefaultAzureCredential();
                case AuthMode.ManagedIdentity:
                    return new ManagedIdentityCredential(
                        new ResourceIdentifier(runSettings.ManagedIdentityResourceId!));

                default:
                    throw new NotSupportedException($"Auth mode:  '{runSettings.AuthMode}'");
            }
        }
    }
}