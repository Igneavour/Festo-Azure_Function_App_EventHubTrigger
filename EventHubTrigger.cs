using System.Text;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.DigitalTwins.Core;
using Azure;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Company.Function
{
    public class EventHubTrigger1
    {
        private readonly ILogger<EventHubTrigger1> _logger;

        // Get environment Variable for Azure Digital Twin?
        private static readonly string? adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly DigitalTwinsClient client = new DigitalTwinsClient(
            new Uri(adtServiceUrl), new DefaultAzureCredential()
        );

        public EventHubTrigger1(ILogger<EventHubTrigger1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(EventHubTrigger1))]
        
        // Run an array of events from the batch
        public async Task Run([EventHubTrigger("festoiothub", Connection = "AzureFunction_RootManageSharedAccessKey_EVENTHUB")] EventData[] events)
        {
            foreach (EventData eventData in events)
            {
                var messageBody = Encoding.UTF8.GetString(eventData.Body.ToArray());
                _logger.LogInformation("Received message: {body}", messageBody);

                try
                {
                    // Suppose messageBody is JSON character string，include G1BG1 and G1BG2 property. TESTING BY LIHAO SO ONLY 2
                    var data = JsonSerializer.Deserialize<PLCData>(messageBody);
                    
                    if (data == null)
                    {
                        _logger.LogError("Deserialized data is null");
                        continue;
                    }
                    
                    _logger.LogInformation("Deserialized data: G1BG1={G1BG1}, G1BG2={G1BG2}", data.G1BG1, data.G1BG2);

                    string digitalTwinId = "PLCout";

                    var updatePatch = new JsonPatchDocument();
                    updatePatch.AppendReplace("/G1BG1", data.G1BG1);
                    updatePatch.AppendReplace("/G1BG2", data.G1BG2);
                    updatePatch.AppendReplace("/G1BG3", data.G1BG3);
                    updatePatch.AppendReplace("/C2BG1", data.C2BG1);
                    updatePatch.AppendReplace("/C2BG2", data.C2BG2);
                    updatePatch.AppendReplace("/C2BG3", data.C2BG3);
                    updatePatch.AppendReplace("/SF1", data.SF1);
                    updatePatch.AppendReplace("/SF2", data.SF2);
                    updatePatch.AppendReplace("/SF3", data.SF3);
                    updatePatch.AppendReplace("/SF4", data.SF4);

                    _logger.LogInformation($"Updating Digital Twin: {digitalTwinId} with G1BG1: {data.G1BG1} and G1BG2: {data.G1BG2}");
                    
                    await client.UpdateDigitalTwinAsync(digitalTwinId, updatePatch);
                    _logger.LogInformation($"Successfully updated Digital Twin: {digitalTwinId}");
                }
                catch (JsonException je)
                {
                    _logger.LogError($"JSON deserialization error: {je.Message}");
                    // handle erroneous sequencing error
                }
                catch (RequestFailedException rfe)
                {
                    _logger.LogError($"Request to ADT failed: {rfe.Message}");
                    // handle case of ADT request failure
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error updating digital twin: {ex.Message}");
                    // handle all other exceptions
                }
            }
        }

    // PLC data in Json format to test sendingg
        private class PLCData
        {
            [JsonPropertyName("G1BG1")]
            public bool G1BG1 {get; set; }
            [JsonPropertyName("G1BG2")]
            public bool G1BG2 {get; set; }
            [JsonPropertyName("G1BG3")]
            public bool G1BG3 {get; set; }
            [JsonPropertyName("C2BG1")]
            public bool C2BG1 {get; set; }
            [JsonPropertyName("C2BG2")]
            public bool C2BG2 {get; set; }
            [JsonPropertyName("C2BG3")]
            public bool C2BG3 {get; set; }
            [JsonPropertyName("SF1")]
            public bool SF1 {get; set; }
            [JsonPropertyName("SF2")]
            public bool SF2 {get; set; }
            [JsonPropertyName("SF3")]
            public bool SF3 {get; set; }
            [JsonPropertyName("SF4")]
            public bool SF4 {get; set; }
        }
    }
}
