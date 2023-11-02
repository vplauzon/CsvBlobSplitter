namespace CsvBlobSplitterConsole.Csv
{
    public interface ICsvSource
    {
        Task<IEnumerable<string>?> RetrieveRowAsync();
    }
}