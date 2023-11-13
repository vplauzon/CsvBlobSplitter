﻿using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs.Specialized;
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
    }
}