using Azure.Storage.Blobs.Specialized;

namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvBlobSource : ICsvSource
    {
        private readonly BlockBlobClient _sourceBlob;
        private readonly BlobCompression _compression;

        public CsvBlobSource(BlockBlobClient sourceBlob, BlobCompression compression)
        {
            _sourceBlob = sourceBlob;
            _compression = compression;
        }

        IAsyncEnumerable<IEnumerable<string>> ICsvSource.RetrieveRowsAsync()
        {
            throw new NotImplementedException();
        }
    }
}