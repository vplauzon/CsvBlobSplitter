using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoBlobSplitLib
{
    public static class EtlRun
    {
        public static async Task RunEtlAsync()
        {
            var stopwatch = new Stopwatch();
            var runSettings = RunSettings.FromEnvironmentVariables();

            stopwatch.Start();
            Console.WriteLine();
            Console.WriteLine($"AuthMode:  {runSettings.AuthMode}");
            Console.WriteLine($"ManagedIdentityResourceId:  {runSettings.ManagedIdentityResourceId}");
            Console.WriteLine($"Format:  {runSettings.Format}");
            Console.WriteLine($"SourceBlob:  {runSettings.SourceBlob}");
            Console.WriteLine($"DestinationBlobPrefix:  {runSettings.DestinationBlobPrefix}");
            Console.WriteLine($"Compression:  {runSettings.InputCompression}");
            Console.WriteLine($"Compression:  {runSettings.OutputCompression}");
            Console.WriteLine($"HasHeaders:  {runSettings.HasHeaders}");
            Console.WriteLine($"MaxMbPerShard:  {runSettings.MaxMbPerShard}");
            Console.WriteLine();
            Console.WriteLine($"Core count:  {Environment.ProcessorCount}");
            Console.WriteLine();

            var etl = EtlFactory.Create(runSettings);

            await etl.ProcessAsync();
            Console.WriteLine($"ETL completed in {stopwatch.Elapsed}");
        }
    }
}