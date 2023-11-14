using Azure.Core;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data;
using Kusto.Ingest;
using KustoBlobSplitLib.LineBased;
using KustoBlobSplitLib.Text;

namespace KustoBlobSplitLib
{
    public class EtlFactory
    {
        public static IEtl Create(RunSettings runSettings)
        {
            var credentials = CredentialFactory.GetCredentials(runSettings);
            var sourceBlobClient = new BlockBlobClient(runSettings.SourceBlob, credentials);

            switch (runSettings.Format)
            {
                case Format.Text:
                    {
                        var subSinkFactory = GetSubSinkFactory(runSettings, credentials);
                        var splitSink = new TextSplitSink(subSinkFactory);
                        var parsingSink = new TextLineParsingSink(
                            splitSink,
                            runSettings.HasHeaders);
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

        private static Func<int, ITextSink> GetSubSinkFactory(
            RunSettings runSettings,
            TokenCredential credentials)
        {
            if (runSettings.KustoIngestUri == null)
            {
                var destinationBlobClient = new BlockBlobClient(
                    runSettings.DestinationBlobPrefix!,
                    credentials);
                var destinationBlobContainer =
                    destinationBlobClient.GetParentBlobContainerClient();
                var destinationBlobPrefix = runSettings.DestinationBlobPrefix!
                    .ToString()
                    .Substring(destinationBlobContainer.Uri.ToString().Length);

                return (shardIndex) => new TextBlobSink(
                    destinationBlobContainer,
                    destinationBlobPrefix,
                    runSettings.OutputCompression,
                    runSettings.MaxMbPerShard,
                    shardIndex);
            }
            else
            {
                var tempFolder = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
                var builder = new KustoConnectionStringBuilder(
                    runSettings.KustoIngestUri.ToString())
                    .WithAadAzureTokenCredentialsAuthentication(credentials);
                var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(builder);

                Directory.CreateDirectory(tempFolder);

                return (shardIndex) => new TextKustoSink(
                    ingestClient,
                    runSettings.KustoDb!,
                    runSettings.KustoTable!,
                    tempFolder,
                    runSettings.OutputCompression,
                    runSettings.MaxMbPerShard,
                    shardIndex);
            }
        }
    }
}