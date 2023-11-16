﻿using Azure.Storage.Blobs;
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
        private readonly BlobClient _shardBlobClient;

        public TextKustoSink(
            RunningContext context,
            int shardIndex,
            string blobNamePrefix)
            : base(context, shardIndex)
        {
            var shardName =
                $"{blobNamePrefix}-{ShardIndex:000000}.txt{GetCompressionExtension()}";
            
            _shardBlobClient = Context
                .RoundRobinIngestStagingContainer()
                .GetBlobClient(shardName);
        }

        protected override async Task<Stream> CreateOutputStreamAsync()
        {
            var writeOptions = new BlobOpenWriteOptions
            {
                BufferSize = WRITING_BUFFER_SIZE
            };

            var blobStream = await _shardBlobClient.OpenWriteAsync(true, writeOptions);

            return blobStream;
        }

        protected override async Task PostWriteAsync()
        {
            var properties = Context.CreateIngestionProperties();
            var tagValue = $"{Context.SourceBlobClient.Uri}-{ShardIndex}";

            properties.IngestByTags = new[] { tagValue };
            properties.IngestIfNotExists = new[] { tagValue };

            await Context.IngestClient!.IngestFromStorageAsync(
                _shardBlobClient.Uri.ToString(),
                properties,
                new StorageSourceOptions
                {
                    CompressionType = Context.BlobSettings.OutputCompression
                });
        }
    }
}