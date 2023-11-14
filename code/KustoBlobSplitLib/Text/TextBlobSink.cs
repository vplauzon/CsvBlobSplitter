using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib.LineBased
{
    internal class TextBlobSink : TextStreamSinkBase
    {
        private const int WRITING_BUFFER_SIZE = 20 * 1024 * 1024;

        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;

        public TextBlobSink(
            BlobContainerClient destinationBlobContainer,
            string destinationBlobPrefix,
            BlobCompression compression,
            int maxMbPerShard,
            int shardIndex)
            : base(compression, maxMbPerShard, shardIndex)
        {
            _destinationBlobContainer = destinationBlobContainer;
            _destinationBlobPrefix = destinationBlobPrefix;
        }

        protected override async Task<Stream> CreateOutputStreamAsync()
        {
            var writeOptions = new BlobOpenWriteOptions
            {
                BufferSize = WRITING_BUFFER_SIZE
            };
            var shardName =
                $"{_destinationBlobPrefix}-{ShardIndex}.txt{GetCompressionExtension()}";
            var shardBlobClient = _destinationBlobContainer.GetBlobClient(shardName);
            var blobStream = await shardBlobClient.OpenWriteAsync(true, writeOptions);

            return blobStream;
        }

        protected override Task PostWriteAsync()
        {
            //   Do nothing as we write to blob directly

            return Task.CompletedTask;
        }

        private string GetCompressionExtension()
        {
            switch (Compression)
            {
                case BlobCompression.None:
                    return string.Empty;
                case BlobCompression.Gzip:
                    return ".gz";

                default:
                    throw new NotSupportedException(Compression.ToString());
            }
        }
    }
}