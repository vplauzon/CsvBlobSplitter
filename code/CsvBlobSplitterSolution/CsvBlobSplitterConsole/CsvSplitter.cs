using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using System.Collections.Immutable;
using System.Globalization;
using System.IO.Compression;

namespace CsvBlobSplitterConsole
{
    internal class CsvSplitter
    {
        private const int BUFFER_SIZE = 200000000;
        private const int BUFFER_LINES = 10000;
        private const int MAX_FILE_SIZE = 10 * 1024 * 1024;

        private readonly bool _hasHeaders;
        private readonly BlobClient _originalBlobClient;
        private readonly BlobContainerClient _targetContainerClient;
        private readonly string _targetBlobsPrefix;
        private readonly BlobCompression _compression;

        public CsvSplitter(
            bool hasHeaders,
            BlobClient originalBlobClient,
            BlobContainerClient targetContainerClient,
            string targetBlobsPrefix,
            BlobCompression compression)
        {
            _hasHeaders = hasHeaders;
            _originalBlobClient = originalBlobClient;
            _targetContainerClient = targetContainerClient;
            _targetBlobsPrefix = targetBlobsPrefix;
            _compression = compression;
        }

        public async Task SplitBlobAsync()
        {
            Console.WriteLine($"Blob {_originalBlobClient.Name}");

            var readOptions = new BlobOpenReadOptions(false)
            {
                BufferSize = BUFFER_SIZE
            };

            using (var readStream = await _originalBlobClient.OpenReadAsync(readOptions))
            using (var uncompressedReader = UncompressStream(readStream))
            using (var textReader = new StreamReader(uncompressedReader))
            using (var csvParser = new CsvParser(textReader, CultureInfo.InvariantCulture))
            {
                var headers = await ReadHeadersAsync(csvParser);

                if (!headers.Any())
                {
                    Console.WriteLine("Empty blob");
                }
                else
                {
                    var targetBlobCount = 0;

                    while (await WriteShardAsync(
                        csvParser,
                        headers,
                        $"{_targetBlobsPrefix}/{targetBlobCount}.csv"))
                    {
                        Console.WriteLine(
                            $"  Shard {_originalBlobClient.Name}-{targetBlobCount} written");
                        ++targetBlobCount;
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
                //case BlobCompression.Gzip:
                case BlobCompression.Zip:
                    var archive = new ZipArchive(readStream);
                    var entries = archive.Entries;

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
            throw new NotImplementedException();
        }

        private async Task<IImmutableList<string>> ReadHeadersAsync(CsvParser csvParser)
        {
            if (_hasHeaders && await csvParser.ReadAsync())
            {
                return csvParser.Record!.ToImmutableArray();
            }
            else
            {
                return ImmutableArray<string>.Empty;
            }
        }

        private async Task<bool> WriteShardAsync(
            CsvParser csvParser,
            IImmutableList<string> headers,
            string shardName)
        {
            var writeOptions = new BlobOpenWriteOptions
            {
                BufferSize = BUFFER_SIZE
            };
            var shardBlobClient = _targetContainerClient.GetBlobClient(shardName);

            using (var writeStream = await shardBlobClient.OpenWriteAsync(true, writeOptions))
            using (var textWriter = new StreamWriter(writeStream))
            {
                if (_hasHeaders)
                {
                    textWriter.WriteLine(string.Join(", ", headers));
                }
                while (writeStream.Position < MAX_FILE_SIZE)
                {
                    for (int i = 0; i < BUFFER_LINES; ++i)
                    {
                        if (await csvParser.ReadAsync())
                        {
                            var record = csvParser.Record!;

                            await textWriter.WriteLineAsync(string.Join(", ", record));
                        }
                        else
                        {   //  Escape the loop:  no more bytes to read
                            return false;
                        }
                    }
                    await textWriter.FlushAsync();
                }
            }

            //  More bytes to read
            return true;
        }
    }
}