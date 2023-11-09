﻿using Azure.Storage.Blobs;
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
    internal class LineBasedBlobSink : ILineBasedSink
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

        private const int WRITING_BUFFER_SIZE = 200 * 1024 * 1024;
        private const int PARALLEL_BLOB_COUNT = 2;

        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;
        private readonly BlobCompression _compression;
        private readonly long _maxBytesPerShard;
        private readonly bool _hasHeaders;

        public LineBasedBlobSink(
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

        bool ILineBasedSink.HasHeaders => _hasHeaders;

        async Task ILineBasedSink.ProcessAsync(WaitingQueue<LineBasedFragment> fragmentQueue)
        {
            var header = _hasHeaders
                ? await DequeueHeaderAsync(fragmentQueue)
                : null;
            var processContext = new ProcessContext(header);
            var processTasks = Enumerable.Range(0, Environment.ProcessorCount)
                .Select(i => ProcessFragmentsAsync(processContext, fragmentQueue, new byte[0]))
                .ToArray();

            await Task.WhenAll(processTasks);
        }

        private async Task ProcessFragmentsAsync(
            ProcessContext processContext,
            WaitingQueue<LineBasedFragment> fragmentQueue,
            byte[] copyBuffer)
        {
            var fragment = await DequeueFragmentAsync(fragmentQueue);

            if (fragment != null)
            {
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
                        fragment.Release();
                    }
                    while (countingStream.Position < _maxBytesPerShard
                    && (fragment = await DequeueFragmentAsync(fragmentQueue)) != null);
                }
                Console.WriteLine($"Sealing blob {shardName}");
                //  Recurrent call
                await ProcessFragmentsAsync(processContext, fragmentQueue, copyBuffer);
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

        private async Task<byte[]?> DequeueHeaderAsync(WaitingQueue<LineBasedFragment> fragmentQueue)
        {
            var fragment = await DequeueFragmentAsync(fragmentQueue);

            if (fragment != null)
            {
                var header = fragment.FragmentBytes.ToArray();

                fragment.Release();

                return header;
            }
            else
            {
                return null;
            }
        }

        private async Task<LineBasedFragment?> DequeueFragmentAsync(
            WaitingQueue<LineBasedFragment> fragmentQueue)
        {
            while (true)
            {
                var awaitNewItemTask = fragmentQueue.AwaitNewItemTask;

                if (fragmentQueue.TryDequeue(out var fragment))
                {
                    return fragment;
                }
                else if (!fragmentQueue.CompletedTask.IsCompleted)
                {
                    await awaitNewItemTask;
                }
                else
                {
                    return null;
                }
            }
        }
    }
}