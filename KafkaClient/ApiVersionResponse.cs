namespace KafkaClient
{
    using System.Collections.Generic;
    using System.IO;

    public class ApiVersionResponse : IResponseMessage
    {
        public void Read(Stream source)
        {
            this.ErrorCode = source.ReadInt16();
            this.ApiVersions = source.ReadMany<ApiVersion>();
        }

        public short ErrorCode { get; private set; }

        public ApiVersion[] ApiVersions { get; private set; }
    }
}
