namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvBlobSource : ICsvSource
    {
        private readonly Uri _sourceBlob;
        private readonly BlobCompression _compression;

        public CsvBlobSource(Uri sourceBlob, BlobCompression compression)
        {
            _sourceBlob = sourceBlob;
            _compression = compression;
        }

        Task<IEnumerable<string>?> ICsvSource.RetrieveRowAsync()
        {
            throw new NotImplementedException();
        }
    }
}