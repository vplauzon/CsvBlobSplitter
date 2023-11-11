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
    internal class TextSource : ISource
    {
#if DEBUG
        private const int STORAGE_BUFFER_SIZE = 10 * 1024 * 1024;
        private const int MIN_STORAGE_FETCH = 1 * 1024 * 1024;
#else
        private const int STORAGE_BUFFER_SIZE = 100 * 1024 * 1024;
        private const int MIN_STORAGE_FETCH = 1 * 1024 * 1024;
#endif
        private const int BUFFER_SIZE = 10 * 1024 * 1024;

        private readonly BlockBlobClient _sourceBlob;
        private readonly BlobCompression _compression;
        private readonly ITextSink _sink;

        public TextSource(
            BlockBlobClient sourceBlob,
            BlobCompression compression,
            ITextSink sink)
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
            var buffer = new byte[BUFFER_SIZE];
            var bufferAvailable = BUFFER_SIZE;
            var readingIndex = 0;
            var fragmentQueue = new WaitingQueue<TextFragment>();
            var releaseQueue = new WaitingQueue<int>();
            var sinkTask = Task.Run(() => _sink.ProcessAsync(
                fragmentQueue,
                releaseQueue));

            Console.WriteLine($"Reading '{_sourceBlob.Uri}'");
            using (var readStream = await _sourceBlob.OpenReadAsync(readOptions))
            using (var uncompressedStream = UncompressStream(readStream))
            {
                while (true)
                {
                    if (bufferAvailable >= MIN_STORAGE_FETCH)
                    {
                        var readLength = await uncompressedStream.ReadAsync(
                            buffer,
                            readingIndex,
                            Math.Min(
                                bufferAvailable,
                                BUFFER_SIZE - readingIndex));

                        if (readLength == 0)
                        {
                            fragmentQueue.Complete();
                            await sinkTask;
                            return;
                        }
                        else
                        {
                            var block = new MemoryBlock(buffer, readingIndex, readLength);

                            fragmentQueue.Enqueue(new TextFragment(block, block));
                            bufferAvailable -= readLength;
                            readingIndex = (readingIndex + readLength) % BUFFER_SIZE;
                        }
                    }
                    else
                    {
                        while (releaseQueue.HasData || bufferAvailable < MIN_STORAGE_FETCH)
                        {
                            var returnLengthResult = await releaseQueue.DequeueAsync();

                            if (returnLengthResult.IsCompleted)
                            {
                                throw new NotSupportedException(
                                    "releaseQueue should never be observed as completed");
                            }
                            bufferAvailable += returnLengthResult.Item;
                        }
                    }
                }
            }
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