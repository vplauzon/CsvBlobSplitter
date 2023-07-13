using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace ConsoleApp2
{
    internal class CsvSplitter
    {
        private readonly BlobContainerClient _sourceContainerClient;
        private readonly BlobContainerClient _targetContainerClient;
        private readonly string _sourcePrefix;
        private readonly string _targetPrefix;
        private readonly string _suffix;

        public CsvSplitter(Uri sourceRoot, string suffix, Uri targetRoot)
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
        }

        public Task SplitAsync()
        {
            throw new NotImplementedException();
        }
    }
}