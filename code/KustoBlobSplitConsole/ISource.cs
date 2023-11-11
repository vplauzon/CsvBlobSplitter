namespace CsvBlobSplitterConsole
{
    internal interface ISource
    {
        Task ProcessSourceAsync();
    }
}