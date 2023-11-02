namespace CsvBlobSplitterConsole.Csv
{
    public interface ICsvSink
    {
        Task PushRowAsync(IEnumerable<string> row);
     
        Task CompleteAsync();
    }
}