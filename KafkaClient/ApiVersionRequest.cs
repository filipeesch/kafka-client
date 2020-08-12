namespace KafkaClient
{
    using System.IO;

    public class ApiVersionRequest : IRequestMessage<ApiVersionResponse>
    {
        public void Write(Stream span)
        {
        }

        public ApiKey ApiKey => ApiKey.ApiVersions;
        public short ApiVersion => 0;
    }
}
