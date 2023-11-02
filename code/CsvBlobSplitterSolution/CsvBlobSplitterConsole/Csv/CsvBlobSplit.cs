using Azure.Storage.Blobs;

namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvBlobSplit : ICsvSink
    {
        private readonly BlobContainerClient _destinationBlobContainer;
        private readonly string _destinationBlobPrefix;
        private readonly IEnumerable<string>? _headers;

        public CsvBlobSplit(
            BlobContainerClient destinationBlobContainer,
            string destinationBlobPrefix,
            IEnumerable<string>? headers)
        {
            _destinationBlobContainer = destinationBlobContainer;
            _destinationBlobPrefix = destinationBlobPrefix;
            _headers = headers;
        }

        Task ICsvSink.PushRowAsync(IEnumerable<string> row)
        {
            throw new NotImplementedException();
        }

        Task ICsvSink.CompleteAsync()
        {
            throw new NotImplementedException();
        }
    }
}