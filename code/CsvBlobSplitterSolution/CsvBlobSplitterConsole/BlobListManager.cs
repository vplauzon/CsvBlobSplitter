using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using System.Collections.Immutable;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.IO.Compression;

namespace CsvBlobSplitterConsole
{
    internal class BlobListManager
    {
        private const int MAX_PARALLEL_BLOBS = 1;

        private readonly BlobContainerClient _sourceContainerClient;
        private readonly BlobContainerClient _targetContainerClient;
        private readonly string _sourcePrefix;
        private readonly string _targetPrefix;
        private readonly string _suffix;
        private readonly bool _hasHeaders;

        public BlobListManager(Uri sourceRoot, string suffix, Uri targetRoot, bool hasHeaders)
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
                var originalBlobClient = _sourceContainerClient.GetBlobClient(blob.Name);
                var targetBlobsPrefix =
                    $"{_targetPrefix}{originalBlobClient.Name.Substring(_sourcePrefix.Length)}";
                var splitter = new CsvSplitter(
                    _hasHeaders,
                    originalBlobClient,
                    _targetContainerClient,
                    targetBlobsPrefix,
                    BlobCompression.Zip);
                var task = splitter.SplitBlobAsync();

                taskList.Add(task);
                if (taskList.Count >= MAX_PARALLEL_BLOBS)
                {
                    var completedTask = await Task.WhenAny(taskList);

                    taskList.Remove(completedTask);
                }
            }

            await Task.WhenAll(taskList);
        }
    }
}