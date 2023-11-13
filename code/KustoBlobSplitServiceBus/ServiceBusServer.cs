using Azure.Identity;
using Azure.Messaging.ServiceBus;
using KustoBlobSplitLib;
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

            await using (var client = new ServiceBusClient(uri.Host, credentials))
            {
                var receiver = client.CreateReceiver(queueName);
                var message = await receiver.PeekMessageAsync();
                var obj = message.Body.ToObjectFromJson<Payload>(new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
            }
        }
    }
}