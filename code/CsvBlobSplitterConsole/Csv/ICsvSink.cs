namespace CsvBlobSplitterConsole.Csv
{
    public interface ICsvSink
    {
        void Start();

        Task PushRowAsync(IEnumerable<string> row);
     
        Task CompleteAsync();
    }
}