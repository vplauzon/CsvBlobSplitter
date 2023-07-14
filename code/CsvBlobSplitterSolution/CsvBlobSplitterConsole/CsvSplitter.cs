using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using System.Collections.Immutable;
using System.Diagnostics.Tracing;
using System.Globalization;

namespace CsvBlobSplitterConsole
{
    internal class CsvSplitter
    {
        private const int MAX_PARALLEL_BLOBS = 5;
        private const int BUFFER_SIZE = 200000000;
        private const int BUFFER_LINES = 10000;
        private const int MAX_FILE_SIZE = 200 * 1024 * 1024;

        private readonly BlobContainerClient _sourceContainerClient;
        private readonly BlobContainerClient _targetContainerClient;
        private readonly string _sourcePrefix;
        private readonly string _targetPrefix;
        private readonly string _suffix;
        private readonly bool _hasHeaders;

        public CsvSplitter(Uri sourceRoot, string suffix, Uri targetRoot, bool hasHeaders)
        {
            var sourceBlobClient = new BlockBlobClient(
                sourceRoot,
                new DefaultAzureCredential());
            var targetBlobClient = new BlockBlobClient(
                targetRoot,
                new DefaultAzureCredential());

            _sourceContainerClient = sourceBlobClient.GetParentBlobContainerClient();
            _targetContainerClient = targetBlobClient.GetParentBlobContainerClient();
            _sourcePrefix = sourceBlobClient.Name;
            _targetPrefix = targetBlobClient.Name;
            _suffix = suffix;
            _hasHeaders = hasHeaders;
        }

        public async Task SplitAsync()
        {
            var blobList = await _sourceContainerClient
                .GetBlobsAsync(prefix: _sourcePrefix)
                .ToListAsync(b => b.Name.EndsWith(_suffix, StringComparison.OrdinalIgnoreCase));
            var taskList = new List<Task>(MAX_PARALLEL_BLOBS);

            Console.WriteLine($"{blobList.Count} blobs found");
            Console.WriteLine();

            foreach (var blob in blobList)
            {
                var task = SplitBlobAsync(_sourceContainerClient.GetBlobClient(blob.Name));

                taskList.Add(task);
                if (taskList.Count >= MAX_PARALLEL_BLOBS)
                {
                    var completedTask = await Task.WhenAny(taskList);

                    taskList.Remove(completedTask);
                }
            }

            await Task.WhenAll(taskList);
        }

        private async Task SplitBlobAsync(BlobClient originalBlobClient)
        {
            Console.WriteLine($"Blob {originalBlobClient.Name}");

            var readOptions = new BlobOpenReadOptions(false)
            {
                BufferSize = BUFFER_SIZE
            };
            var targetBlobsPrefix =
                $"{_targetPrefix}{originalBlobClient.Name.Substring(_sourcePrefix.Length)}";

            using (var readStream = await originalBlobClient.OpenReadAsync(readOptions))
            using (var textReader = new StreamReader(readStream))
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
                        $"{targetBlobsPrefix}/{targetBlobCount}.csv"))
                    {
                        Console.WriteLine(
                            $"  Shard {originalBlobClient.Name}-{targetBlobCount} written");
                        ++targetBlobCount;
                    }
                }
            }
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