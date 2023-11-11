using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using System.Globalization;
using System.IO.Compression;

namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvBlobSource : ICsvSource
    {
        private const int BUFFER_SIZE = 200000000;

        private readonly BlockBlobClient _sourceBlob;
        private readonly BlobCompression _compression;

        public CsvBlobSource(BlockBlobClient sourceBlob, BlobCompression compression)
        {
            _sourceBlob = sourceBlob;
            _compression = compression;
        }

        async IAsyncEnumerable<IEnumerable<string>> ICsvSource.RetrieveRowsAsync()
        {
            var readOptions = new BlobOpenReadOptions(false)
            {
                BufferSize = BUFFER_SIZE
            };

            using (var readStream = await _sourceBlob.OpenReadAsync(readOptions))
            using (var uncompressedReader = UncompressStream(readStream))
            using (var textReader = new StreamReader(uncompressedReader))
            using (var csvParser = new CsvParser(textReader, CultureInfo.InvariantCulture))
            {
                while (await csvParser.ReadAsync())
                {
                    var record = csvParser.Record!;

                    yield return record;
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