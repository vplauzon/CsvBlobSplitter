namespace CsvBlobSplitterConsole.Csv
{
    internal class CsvBlobSplit : ICsvSink
    {
        private readonly Uri? _destinationBlobPrefix;
        private readonly IEnumerable<string>? _headers;

        public CsvBlobSplit(Uri destinationBlobPrefix, IEnumerable<string>? headers)
        {
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