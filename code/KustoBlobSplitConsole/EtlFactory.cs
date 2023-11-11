using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs.Specialized;
using CsvBlobSplitterConsole.LineBased;
using CsvBlobSplitterConsole.Text;

namespace CsvBlobSplitterConsole
{
    internal class EtlFactory
    {
        public static IEtl Create(RunSettings runSettings)
        {
            var credentials = GetCredentials(runSettings);
            var sourceBlobClient = new BlockBlobClient(runSettings.SourceBlob, credentials);
            var destinationBlobClient = new BlockBlobClient(
                runSettings.DestinationBlobPrefix!,
                credentials);
            var destinationBlobContainer =
                destinationBlobClient.GetParentBlobContainerClient();
            var destinationBlobPrefix = runSettings.DestinationBlobPrefix!
                .ToString()
                .Substring(destinationBlobContainer.Uri.ToString().Length);

            switch (runSettings.Format)
            {
                case Format.Text:
                    {
                        var blobSink = new TextBlobSink(
                            destinationBlobContainer,
                            destinationBlobPrefix,
                            runSettings.OutputCompression,
                            runSettings.MaxMbPerShard,
                            runSettings.HasHeaders);
                        var parsingSink = new TextLineParsingSink(blobSink);
                        var source = new TextSource(
                            sourceBlobClient,
                            runSettings.InputCompression,
                            parsingSink);

                        return new SingleSourceEtl(source);
                    }

                default:
                    throw new NotSupportedException($"Format '{runSettings.Format}'");
            }
        }

        private static TokenCredential GetCredentials(RunSettings runSettings)
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