using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal class TextKustoSink : TextStreamSinkBase
    {
        private readonly string _blobNamePrefix;

        public TextKustoSink(
            RunningContext context,
            int shardIndex,
            string blobNamePrefix)
            : base(context, shardIndex)
        {
            _blobNamePrefix = blobNamePrefix;
        }

        protected override async Task<Stream> CreateOutputStreamAsync()
        {
            var writeOptions = new BlobOpenWriteOptions
            {
                BufferSize = WRITING_BUFFER_SIZE
            };

            var shardBlobClient = GetShardBlobClient();
            var blobStream = await shardBlobClient.OpenWriteAsync(true, writeOptions);

            return blobStream;
        }

        protected override async Task PostWriteAsync()
        {
            var shardBlobClient = GetShardBlobClient();
            var properties = Context.CreateIngestionProperties();
            var tagValue = $"{Context.SourceBlobClient.Uri}-{ShardIndex}";

            properties.IngestByTags = new[] { tagValue };
            properties.IngestIfNotExists = new[] { tagValue };

            await Context.IngestClient!.IngestFromStorageAsync(
                shardBlobClient.Uri.ToString(),
                properties,
                new StorageSourceOptions
                {
                    CompressionType = Context.BlobSettings.OutputCompression
                });
        }

        private BlobClient GetShardBlobClient()
        {
            var shardName =
                $"{_blobNamePrefix}-{ShardIndex}.txt{GetCompressionExtension()}";
            var shardBlobClient = Context
                .RoundRobinIngestStagingContainer()
                .GetBlobClient(shardName);

            return shardBlobClient;
        }
    }
}