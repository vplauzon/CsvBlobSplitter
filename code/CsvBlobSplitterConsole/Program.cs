namespace CsvBlobSplitterConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            Console.WriteLine();
            Console.WriteLine($"Format:  {runSettings.Format}");
            Console.WriteLine($"SourceBlob:  {runSettings.SourceBlob}");
            Console.WriteLine($"DestinationBlobPrefix:  {runSettings.DestinationBlobPrefix}");
            Console.WriteLine($"Compression:  {runSettings.InputCompression}");
            Console.WriteLine($"Compression:  {runSettings.OutputCompression}");
            Console.WriteLine($"HasCsvHeaders:  {runSettings.HasHeaders}");
            Console.WriteLine($"MaxRowsPerShard:  {runSettings.MaxRowsPerShard}");
            Console.WriteLine($"MaxMbPerShard:  {runSettings.MaxMbPerShard}");
            Console.WriteLine();

            var etl = EtlFactory.Create(runSettings);

            await etl.ProcessAsync();
        }
    }
}