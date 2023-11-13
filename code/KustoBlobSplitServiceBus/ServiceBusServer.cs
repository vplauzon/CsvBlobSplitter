using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Kusto.Ingest.Exceptions;
using KustoBlobSplitLib;
using Microsoft.Identity.Client.AppConfig;
using System.Text.Json;

namespace KustoBlobSplitServiceBus
{
    public static class ServiceBusServer
    {
        public static async Task RunServerAsync(RunSettings runSettings)
        {
            if (string.IsNullOrWhiteSpace(runSettings.ServiceBusQueueUrl))
            {
                throw new ArgumentNullException(nameof(runSettings.ServiceBusQueueUrl));
            }
            var uri = new Uri(runSettings.ServiceBusQueueUrl, UriKind.Absolute);
            var queueName = uri
                .Segments
                .Where(s => s != "/")
                .FirstOrDefault();
            var credentials = CredentialFactory.GetCredentials(runSettings);

            while (true)
            {
                await using (var client = new ServiceBusClient(uri.Host, credentials))
                {
                    var receiver = client.CreateReceiver(queueName);
                    var message = await receiver.PeekMessageAsync();
                    var payload = message.Body.ToObjectFromJson<Payload>(new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    if (payload.Data == null
                        || payload.Time == null
                        || payload.Data.BlobUrl == null
                        || !payload.Data.BlobUrl.IsAbsoluteUri)
                    {
                        throw new InvalidDataException(
                            "Queue payload invalid:  this isn't an Event Grid Cloud event");
                    }

                    Console.WriteLine();
                    Console.WriteLine($"Queued blob:  {payload.Data?.BlobUrl}");
                    Console.WriteLine($"Enqueued time:  {payload.Time}");

                    var subSettings = runSettings
                        .OverrideSourceBlob(payload.Data?.BlobUrl!);

                    await EtlRun.RunEtlAsync(subSettings);
                    //await receiver.CompleteMessageAsync(message);
                }
            }
        }
    }
}