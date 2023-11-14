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
    internal class TextBlobSink : ITextSink
    {
        private const int WRITING_BUFFER_SIZE = 20 * 1024 * 1024;

        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;
        private readonly BlobCompression _compression;
        private readonly long _maxBytesPerShard;
        private readonly int _shardIndex;

        public TextBlobSink(
            BlobContainerClient destinationBlobContainer,
            string destinationBlobPrefix,
            BlobCompression compression,
            int maxMbPerShard,
            int shardIndex)
        {
            _destinationBlobContainer = destinationBlobContainer;
            _destinationBlobPrefix = destinationBlobPrefix;
            _compression = compression;
            _maxBytesPerShard = ((long)maxMbPerShard) * 1024 * 1024;
            _shardIndex = shardIndex;
        }

        async Task ITextSink.ProcessAsync(
            TextFragment? headerFragment,
            IWaitingQueue<TextFragment> fragmentQueue,
            IWaitingQueue<int> releaseQueue)
        {
            var copyBuffer = new byte[0];
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            //  We pre-fetch a fragment not to create an empty blob
            var fragmentResult = await fragmentQueue.DequeueAsync();

            if (!fragmentResult.IsCompleted)
            {
                var fragment = fragmentResult.Item!;
                var writeOptions = new BlobOpenWriteOptions
                {
                    BufferSize = WRITING_BUFFER_SIZE
                };
                var shardName =
                    $"{_destinationBlobPrefix}-{_shardIndex}.csv{GetCompressionExtension()}";
                var shardBlobClient = _destinationBlobContainer.GetBlobClient(shardName);

                using (var blobStream = await shardBlobClient.OpenWriteAsync(true, writeOptions))
                using (var compressedStream = CompressedStream(blobStream))
                using (var countingStream = new ByteCountingStream(compressedStream))
                {
                    if (headerFragment != null)
                    {
                        copyBuffer = await WriteFragmentAsync(
                            copyBuffer,
                            headerFragment,
                            countingStream);
                    }
                    do
                    {
                        copyBuffer = await WriteFragmentAsync(
                            copyBuffer,
                            fragment,
                            countingStream);
                        releaseQueue.Enqueue(fragment.FragmentBytes.Count());
                    }
                    while (countingStream.Position < _maxBytesPerShard
                    && !(fragmentResult = await fragmentQueue.DequeueAsync()).IsCompleted);
                }
                Console.WriteLine($"Sealing blob {shardName} ({stopwatch.Elapsed})");
            }
        }

        private async Task<byte[]> WriteFragmentAsync(
            byte[] copyBuffer,
            TextFragment fragment,
            Stream stream)
        {
            if (fragment.FragmentBlock != null)
            {
                await stream.WriteAsync(
                    fragment.FragmentBlock.Buffer,
                    fragment.FragmentBlock.Offset,
                    fragment.FragmentBlock.Count);
            }
            else
            {
                copyBuffer = CopyBytes(copyBuffer, fragment.FragmentBytes);
                await stream.WriteAsync(
                    copyBuffer,
                    0,
                    fragment.FragmentBytes.Count());
            }

            return copyBuffer;
        }

        private string GetCompressionExtension()
        {
            switch (_compression)
            {
                case BlobCompression.None:
                    return string.Empty;
                case BlobCompression.Gzip:
                    return ".gz";

                default:
                    throw new NotSupportedException(_compression.ToString());
            }
        }

        private byte[] CopyBytes(byte[] copyBuffer, IEnumerable<byte> fragmentBytes)
        {
            var i = 0;

            if (copyBuffer.Length < fragmentBytes.Count())
            {
                copyBuffer = new byte[fragmentBytes.Count()];
            }
            foreach (var b in fragmentBytes)
            {
                copyBuffer[i++] = b;
            }

            return copyBuffer;
        }

        private Stream CompressedStream(Stream stream)
        {
            switch (_compression)
            {
                case BlobCompression.None:
                    return stream;
                case BlobCompression.Gzip:
                    return new GZipStream(stream, CompressionMode.Compress);

                default:
                    throw new NotSupportedException(_compression.ToString());
            }
        }
    }
}