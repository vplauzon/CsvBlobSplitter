using Azure.Identity;
using Azure.Storage.Blobs.Specialized;
using CsvBlobSplitterConsole.Csv;

namespace CsvBlobSplitterConsole
{
    internal class EtlFactory
    {
        public static IEtl Create(RunSettings runSettings)
        {
            var credentials = new DefaultAzureCredential();
            var sourceBlobClient = new BlockBlobClient(runSettings.SourceBlob, credentials);
            var destinationBlobClient = new BlockBlobClient(
                runSettings.DestinationBlobPrefix!,
                credentials);
            var destinationBlobContainer =
                destinationBlobClient.GetParentBlobContainerClient();
            var destinationBlobPrefix = runSettings.DestinationBlobPrefix!
                .ToString()
                .Substring(destinationBlobContainer.Uri.ToString().Length);
            var source = new CsvBlobSource(sourceBlobClient, runSettings.InputCompression);

            return new CsvEtl(
                source,
                (headers) => new CsvBlobSink(
                    destinationBlobContainer,
                    destinationBlobPrefix,
                    runSettings.MaxRowsPerShard,
                    runSettings.MaxMbPerShard,
                    headers),
                runSettings.HasHeaders);
        }
    }
}