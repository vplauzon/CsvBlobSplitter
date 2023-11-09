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

namespace CsvBlobSplitterConsole.LineBased
{
    internal class TextBlobSink : ITextSink
    {
        #region Inner Types
        private class ProcessContext
        {
            private volatile int _counter = 0;

            public ProcessContext(byte[]? header)
            {
                Header = header;
            }

            public byte[]? Header { get; }

            public int GetNewCounterValue()
            {
                return Interlocked.Increment(ref _counter);
            }
        }
        #endregion

        private const int WRITING_BUFFER_SIZE = 20 * 1024 * 1024;

        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;
        private readonly BlobCompression _compression;
        private readonly long _maxBytesPerShard;
        private readonly bool _hasHeaders;

        public TextBlobSink(
            BlobContainerClient destinationBlobContainer,
            string destinationBlobPrefix,
            BlobCompression compression,
            int maxMbPerShard,
            bool hasHeaders)
        {
            _destinationBlobContainer = destinationBlobContainer;
            _destinationBlobPrefix = destinationBlobPrefix;
            _compression = compression;
            _maxBytesPerShard = ((long)maxMbPerShard) * 1024 * 1024;
            _hasHeaders = hasHeaders;
        }

        bool ITextSink.HasHeaders => _hasHeaders;

        async Task ITextSink.ProcessAsync(
            WaitingQueue<TextFragment> fragmentQueue,
            WaitingQueue<int> releaseQueue)
        {
            var header = _hasHeaders
                ? await DequeueHeaderAsync(fragmentQueue, releaseQueue)
                : null;
            var processContext = new ProcessContext(header);
            var parallelism = (int)Math.Ceiling(1.5 * Environment.ProcessorCount);
            var processTasks = Enumerable.Range(0, parallelism)
                .Select(i => ProcessFragmentsAsync(
                    processContext,
                    fragmentQueue,
                    releaseQueue,
                    new byte[0]))
                .ToArray();

            await Task.WhenAll(processTasks);
        }

        private async Task ProcessFragmentsAsync(
            ProcessContext processContext,
            WaitingQueue<TextFragment> fragmentQueue,
            WaitingQueue<int> releaseQueue,
            byte[] copyBuffer)
        {
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
                var counter = processContext.GetNewCounterValue();
                var shardName =
                    $"{_destinationBlobPrefix}-{counter}.csv{GetCompressionExtension()}";
                var shardBlobClient = _destinationBlobContainer.GetBlobClient(shardName);

                using (var blobStream = await shardBlobClient.OpenWriteAsync(true, writeOptions))
                using (var compressedStream = CompressedStream(blobStream))
                using (var countingStream = new ByteCountingStream(compressedStream))
                {
                    if (processContext.Header != null)
                    {
                        await countingStream.WriteAsync(processContext.Header);
                    }
                    do
                    {
                        if (fragment.FragmentBlock != null)
                        {
                            await countingStream.WriteAsync(
                                fragment.FragmentBlock.Buffer,
                                fragment.FragmentBlock.Offset,
                                fragment.FragmentBlock.Count);
                        }
                        else
                        {
                            copyBuffer = CopyBytes(copyBuffer, fragment.FragmentBytes);
                            await countingStream.WriteAsync(
                                copyBuffer,
                                0,
                                fragment.FragmentBytes.Count());
                        }
                        releaseQueue.Enqueue(fragment.FragmentBytes.Count());
                    }
                    while (countingStream.Position < _maxBytesPerShard
                    && !(fragmentResult = await fragmentQueue.DequeueAsync()).IsCompleted);
                }
                Console.WriteLine($"Sealing blob {shardName} ({stopwatch.Elapsed})");
                //  Recurrent call
                await ProcessFragmentsAsync(
                    processContext,
                    fragmentQueue,
                    releaseQueue,
                    copyBuffer);
            }
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

        private async Task<byte[]?> DequeueHeaderAsync(
            WaitingQueue<TextFragment> fragmentQueue,
            WaitingQueue<int> releaseQueue)
        {
            var fragmentResult = await fragmentQueue.DequeueAsync();

            if (!fragmentResult.IsCompleted)
            {
                var header = fragmentResult.Item!.FragmentBytes.ToArray();

                releaseQueue.Enqueue(header.Length);

                return header;
            }
            else
            {
                return null;
            }
        }
    }
}