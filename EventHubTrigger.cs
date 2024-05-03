using System.Text;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.DigitalTwins.Core;
using Azure;
using Azure.Messaging.EventGrid;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Devices;

namespace Company.Function
{
    public class EventHubTrigger1
    {
        private readonly ILogger<EventHubTrigger1> _logger;
        private static readonly string? adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly DigitalTwinsClient client = new DigitalTwinsClient(
            new Uri(adtServiceUrl), new DefaultAzureCredential()
        );

        public EventHubTrigger1(ILogger<EventHubTrigger1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(EventHubTrigger1))]
        public async Task Run([EventHubTrigger("festoiothub", Connection = "AzureFunction_RootManageSharedAccessKey_EVENTHUB")] EventData[] events)
        {
            foreach (EventData eventData in events)
            {
                var messageBody = Encoding.UTF8.GetString(eventData.Body.ToArray());
                _logger.LogInformation("Received message: {body}", messageBody);

                try
                {
                    //假设 messageBody 是一个 JSON 字符串，包含 status 和 temperature 属性
                    var data = JsonSerializer.Deserialize<TemperatureSensorData>(messageBody);

                    string digitalTwinId = "TestTwin1";

                    var updatePatch = new JsonPatchDocument();
                    updatePatch.AppendReplace("/status", data.Status);
                    updatePatch.AppendReplace("/temperature", data.Temperature);

                    await client.UpdateDigitalTwinAsync(digitalTwinId, updatePatch);
                    _logger.LogInformation($"Updated Digital Twin: {digitalTwinId} with status: {data.Status} and temperature: {data.Temperature}");
                }
                catch (JsonException je)
                {
                    _logger.LogError($"JSON deserialization error: {je.Message}");
                    // 处理反序列化错误
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error updating digital twin: {ex.Message}");
                    // 处理其他异常
                }
            }
        }

        private class TemperatureSensorData
        {
            [JsonPropertyName("status")]
            public string? Status {get; set; }
            [JsonPropertyName("temperature")]
            public double Temperature {get; set; }
        }
    }

    public class DigitalTwinsToIoTHubFunction
    {
        private readonly ILogger<DigitalTwinsToIoTHubFunction> log;
        private static readonly ServiceClient serviceClient = ServiceClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("IOTHUB_CONNECTION_STRING"));

        public DigitalTwinsToIoTHubFunction(ILogger<DigitalTwinsToIoTHubFunction> logger)
        {
            log = logger;
        }

        [Function("DigitalTwinsToIoTHubFunction")]
        public async Task Run([EventGridTrigger] EventGridEvent eventGridEvent)
        {
            log.LogInformation("EventGrid trigger function processed a request.");

            string jsonData = eventGridEvent.Data.ToString();
            log.LogInformation($"Raw event data: {jsonData}");

            // Try to deserialize the event data into the expected DataWithPatch structure
            var eventDataWrapper = JsonSerializer.Deserialize<EventDataWrapper>(jsonData);

            if (eventDataWrapper?.Data?.Patch == null)
            {
                log.LogError("Patch data is null or missing.");
                return;
            }

            // Extract the status and temperature values from the patch operations
            string? status = eventDataWrapper.Data.Patch.FirstOrDefault(p => p.Path == "/status")?.Value.GetString();
            double? temperature = eventDataWrapper.Data.Patch.FirstOrDefault(p => p.Path == "/temperature")?.Value.GetDouble();

            if (string.IsNullOrEmpty(status) || !temperature.HasValue)
            {
                log.LogWarning("Status or temperature is missing or invalid in the patch.");
                return;
            }

            // 构建一个新的消息对象来发送
            var messageObject = new
            {
                status = status,
                temperature = temperature.Value
            };

            // 构造你想发送的消息
            var messageString = JsonSerializer.Serialize(messageObject);
            var message = new Message(Encoding.UTF8.GetBytes(messageString));

            //发送信息给IoT Hub
            await serviceClient.SendAsync("FestoDevice", message);
            log.LogInformation($"Message sent to IoT Hub for device FestoDevice: {messageString}");
        }

        // Class to represent a single operation in a patch
        private class PatchOperation
        {
            [JsonPropertyName("op")]
            public string? Op { get; set; }

            [JsonPropertyName("path")]
            public string? Path { get; set; }

            [JsonPropertyName("value")]
            public JsonElement Value { get; set; }
        }

        // Class to represent the overall patch structure received in the event data
        private class DataWithPatch
        {
            [JsonPropertyName("patch")]
            public List<PatchOperation>? Patch { get; set; }
        }

        private class EventDataWrapper
        {
            [JsonPropertyName("data")]
            public DataWithPatch? Data {get; set; }
        }
    }
}
