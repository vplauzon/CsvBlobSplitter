﻿using System.Diagnostics;

namespace CsvBlobSplitterConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var stopwatch = new Stopwatch();
            var runSettings = RunSettings.FromEnvironmentVariables();

            stopwatch.Start();
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
            Console.WriteLine($"ETL completed in {stopwatch.Elapsed}");
        }
    }
}