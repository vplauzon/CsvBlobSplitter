using KustoBlobSplitLib;
using KustoBlobSplitServiceBus;
using Microsoft.Extensions.Hosting;

namespace KustoBlobSplitServer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            var app = builder.Build();

            // Configure the HTTP request pipeline.

            app.MapGet("/", (HttpContext httpContext) =>
            {
                return string.Empty;
            });

            var webServerTask = app.RunAsync();
            var runSettings = RunSettings.FromEnvironmentVariables();

            runSettings.WriteOutSettings();
            if (string.IsNullOrWhiteSpace(runSettings.ServiceBusQueueUrl))
            {   //  Run one ETL
                await EtlRun.RunEtlAsync(runSettings);
            }
            else
            {   //  Run Service Bus server picking up tasks
                await ServiceBusServer.RunServerAsync(runSettings);
            }

            //  Stop web server
            await Task.WhenAll(app.StopAsync(), webServerTask);
        }
    }
}