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
        public static async Task RunEtlAsync(RunSettings runSettings)
        {
            var stopwatch = new Stopwatch();

            runSettings.WriteOutSettings();
            stopwatch.Start();

            var etl = EtlFactory.Create(runSettings);

            await etl.ProcessAsync();
            Console.WriteLine($"ETL completed in {stopwatch.Elapsed}");
        }
    }
}