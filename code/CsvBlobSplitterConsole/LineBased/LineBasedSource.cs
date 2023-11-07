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
        private const int STORAGE_BUFFER_SIZE = 100000000;
        private const int PARSING_BUFFER_SIZE = 100000000;

        private readonly BlockBlobClient _sourceBlob;
        private readonly BlobCompression _compression;

        public LineBasedSource(BlockBlobClient sourceBlob, BlobCompression compression)
        {
            _sourceBlob = sourceBlob;
            _compression = compression;
        }

        async Task ISource.ProcessSourceAsync()
        {
            var readOptions = new BlobOpenReadOptions(false)
            {
                BufferSize = STORAGE_BUFFER_SIZE
            };
            var buffer = new byte[STORAGE_BUFFER_SIZE];
            var readingIndex = 0;
            //var parsingIndex = 0;
            var sinkIndex = 0;

            using (var readStream = await _sourceBlob.OpenReadAsync(readOptions))
            using (var uncompressedStream = UncompressStream(readStream))
            {
                var maxReadSize = GetAvailBufferSize(readingIndex, sinkIndex);

                if (maxReadSize > 0)
                {
                    var readLength = await uncompressedStream.ReadAsync(
                        buffer,
                        readingIndex,
                        maxReadSize);

                    throw new NotImplementedException();
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        private int GetAvailBufferSize(int readingIndex, int sinkIndex)
        {
            return readingIndex < sinkIndex
                ? sinkIndex - readingIndex
                : PARSING_BUFFER_SIZE + sinkIndex - readingIndex;
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