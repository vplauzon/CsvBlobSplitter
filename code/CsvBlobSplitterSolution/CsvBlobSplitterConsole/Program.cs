namespace CsvBlobSplitterConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();
            var etl = EtlFactory.Create(runSettings);

            await etl.ProcessAsync();
        }
    }
}