using Azure.Identity;
using Azure.Storage.Blobs.Specialized;
using CsvBlobSplitterConsole.Csv;
using CsvBlobSplitterConsole.LineBased;
using CsvBlobSplitterConsole.Text;
using System.Reflection.PortableExecutable;

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
                case Format.Csv:
                    {
                        var source = new CsvBlobSource(
                            sourceBlobClient,
                            runSettings.InputCompression);

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

                default:
                    throw new NotSupportedException($"Format '{runSettings.Format}'");
            }
        }
    }
}