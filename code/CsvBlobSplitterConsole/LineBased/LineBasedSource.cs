using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole.LineBased
{
    internal class LineBasedSource : ISource
    {
        #region Inner Types
        #endregion

        private const int STORAGE_BUFFER_SIZE =
#if DEBUG
            10 * 1024 * 1024;
#else
            100 * 1024 * 1024;
#endif
        private const int PARSING_BUFFER_SIZE =
#if DEBUG
            10 * 1024 * 1024;
#else
            10 * 1024 * 1024;
#endif
        private const int SINK_BUFFER_SIZE = 1024 * 1024;

        private readonly BlockBlobClient _sourceBlob;
        private readonly BlobCompression _compression;
        private readonly ILineBasedSink _sink;

        public LineBasedSource(
            BlockBlobClient sourceBlob,
            BlobCompression compression,
            ILineBasedSink sink)
        {
            _sourceBlob = sourceBlob;
            _compression = compression;
            _sink = sink;
        }

        async Task ISource.ProcessSourceAsync()
        {
            var readOptions = new BlobOpenReadOptions(false)
            {
                BufferSize = STORAGE_BUFFER_SIZE
            };
            var buffer = new byte[PARSING_BUFFER_SIZE];
            var readingIndex = 0;
            var sinkIndex = 0;
            var bufferQueue = new WaitingQueue<int>();
            var bufferReturnQueue = new WaitingQueue<int>();
            var parsingTask = Task.Run(() => ParseBufferAsync(
                buffer,
                bufferQueue,
                bufferReturnQueue));

            using (var readStream = await _sourceBlob.OpenReadAsync(readOptions))
            using (var uncompressedStream = UncompressStream(readStream))
            {
                Console.WriteLine($"Reading '{_sourceBlob.Uri}'");
                while (true)
                {
                    var maxReadSize = IndexDistance(readingIndex, sinkIndex);

                    if (maxReadSize > 0)
                    {
                        var readLength = await uncompressedStream.ReadAsync(
                            buffer,
                            readingIndex,
                            maxReadSize);

                        if (readLength == 0)
                        {
                            bufferQueue.Complete();
                            await parsingTask;
                            return;
                        }
                        else
                        {
                            bufferQueue.Enqueue(readLength);
                        }
                        readingIndex += readLength;
                        readingIndex = readingIndex % PARSING_BUFFER_SIZE;
                    }
                    else
                    {
                        var newItemTask = bufferReturnQueue.AwaitNewItemTask;

                        while (bufferReturnQueue.TryDequeue(out var returnLength))
                        {
                            sinkIndex += returnLength;
                        }
                        sinkIndex = sinkIndex % PARSING_BUFFER_SIZE;
                        if (IndexDistance(readingIndex, sinkIndex) == 0)
                        {
                            await newItemTask;
                        }
                    }
                }
            }
        }

        private async Task ParseBufferAsync(
            byte[] buffer,
            WaitingQueue<int> bufferQueue,
            WaitingQueue<int> bufferReturnQueue)
        {
            var fragmentStartIndex = 0;
            var parsingIndex = 0;
            var lastLineStopIndex = (int?)null;
            var isFirstLine = true;
            var fragmentQueue = new Queue<LineBasedFragment>();
            var sharedFragmentQueue = new WaitingQueue<LineBasedFragment>();
            var sinkTask = _sink.ProcessAsync(sharedFragmentQueue);

            while (!bufferQueue.CompletedTask.IsCompleted)
            {
                var newItemTask = bufferQueue.AwaitNewItemTask;

                if (bufferQueue.TryDequeue(out var readLength))
                {
                    for (var i = 0; i != readLength; ++i)
                    {
                        if (buffer[parsingIndex] == '\n')
                        {
                            if (IndexDistance(fragmentStartIndex, parsingIndex) > SINK_BUFFER_SIZE
                                || (isFirstLine && _sink.HasHeaders))
                            {
                                PushFragment(
                                    buffer,
                                    fragmentStartIndex,
                                    parsingIndex + 1,
                                    fragmentQueue,
                                    sharedFragmentQueue);
                                fragmentStartIndex = parsingIndex + 1;
                                lastLineStopIndex = null;
                                isFirstLine = false;
                            }
                            else
                            {
                                lastLineStopIndex = parsingIndex;
                            }
                        }
                        ++parsingIndex;
                        parsingIndex = parsingIndex % PARSING_BUFFER_SIZE;
                    }
                }
                else if (lastLineStopIndex != null)
                {
                    PushFragment(
                        buffer,
                        fragmentStartIndex,
                        lastLineStopIndex.Value + 1,
                        fragmentQueue,
                        sharedFragmentQueue);
                    fragmentStartIndex = parsingIndex + 1;
                    lastLineStopIndex = null;
                    await Task.WhenAny(bufferQueue.CompletedTask, newItemTask);
                }
                ReturnBuffer(fragmentQueue, bufferReturnQueue);
            }
            sharedFragmentQueue.Complete();
            bufferReturnQueue.Complete();
            await sinkTask;
        }

        private void ReturnBuffer(
            Queue<LineBasedFragment> fragmentQueue,
            WaitingQueue<int> bufferReturnQueue)
        {
            var totalLength = 0;

            while (fragmentQueue.TryPeek(out var fragment)
                && fragment.ReleasedTask.IsCompleted)
            {
                fragment = fragmentQueue.Dequeue();
                totalLength += fragment.FragmentBytes.Count();
            }
            if (totalLength > 0)
            {
                bufferReturnQueue.Enqueue(totalLength);
            }
        }

        private void PushFragment(
            byte[] buffer,
            int startIndex,
            int endIndex,
            Queue<LineBasedFragment> fragmentQueue,
            WaitingQueue<LineBasedFragment> sharedFragmentQueue)
        {
            var fragment = CreateFragment(buffer, startIndex, endIndex);

            sharedFragmentQueue.Enqueue(fragment);
            fragmentQueue.Enqueue(fragment);
        }

        private LineBasedFragment CreateFragment(byte[] buffer, int startIndex, int endIndex)
        {
            if (startIndex < endIndex)
            {
                var block = new MemoryBlock(buffer, startIndex, endIndex - startIndex);

                return new LineBasedFragment(block, block);
            }
            else
            {
                var block1 = new MemoryBlock(buffer, startIndex, PARSING_BUFFER_SIZE - startIndex);
                var block2 = new MemoryBlock(buffer, 0, endIndex);

                return new LineBasedFragment(block1.Concat(block2), null);
            }
        }

        private int IndexDistance(int index1, int index2)
        {
            return index1 < index2
                ? index2 - index1
                : PARSING_BUFFER_SIZE + index2 - index1;
        }

        private Stream UncompressStream(Stream readStream)
        {
            switch (_compression)
            {
                case BlobCompression.None:
                    return readStream;
                case BlobCompression.Gzip:
                    return new GZipStream(readStream, CompressionMode.Decompress);
                case BlobCompression.Zip:
                    var archive = new ZipArchive(readStream);
                    var entries = archive
                        .Entries
                        .Where(e => !string.IsNullOrWhiteSpace(e.Name))
                        .Where(e => e.Length > 0);

                    if (!entries.Any())
                    {
                        throw new InvalidDataException(
                            $"Archive (zipped blob) doesn't contain any file");
                    }
                    else
                    {
                        return entries.First().Open();
                    }

                default:
                    throw new NotSupportedException(_compression.ToString());
            }
        }
    }
}