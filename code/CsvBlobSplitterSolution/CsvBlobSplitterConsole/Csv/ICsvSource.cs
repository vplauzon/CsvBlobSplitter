namespace CsvBlobSplitterConsole.Csv
{
    public interface ICsvSource
    {
        IAsyncEnumerable<IEnumerable<string>> RetrieveRowsAsync();
    }
}