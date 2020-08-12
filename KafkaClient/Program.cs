namespace KafkaClient
{
    using System;
    using System.Threading.Tasks;

    class Program
    {
        static async Task Main(string[] args)
        {
            var connection = new KafkaHostConnection("localhost", 9092);

            var apiVersionTask = connection.SendAsync(
                new ApiVersionRequest(),
                TimeSpan.FromSeconds(30));

            var topicMetadataTask = connection.SendAsync(
                new TopicMetadataRequest(new[] { "test-topic", "test-gzip", "test-json" }),
                TimeSpan.FromSeconds(30));

            var topicMetadata = await topicMetadataTask;
            var apiVersion = await apiVersionTask;

            await Task.Delay(5000);
        }
    }
}
