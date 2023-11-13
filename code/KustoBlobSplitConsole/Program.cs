using System.Diagnostics;
using KustoBlobSplitLib;
using KustoBlobSplitServiceBus;

namespace KustoBlobSplitConsole
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var runSettings = RunSettings.FromEnvironmentVariables();

            if (string.IsNullOrWhiteSpace(runSettings.ServiceBusQueueUrl))
            {   //  Run one ETL
                await EtlRun.RunEtlAsync(runSettings);
            }
            else
            {   //  Run Service Bus server picking up tasks
                await ServiceBusServer.RunServerAsync(runSettings);
            }
        }
    }
}