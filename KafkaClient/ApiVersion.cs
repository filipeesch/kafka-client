namespace KafkaClient
{
    using System.IO;

    public class ApiVersion : IResponse
    {
        public short ApiKey { get; private set; }

        public short MinVersion { get; private set; }

        public short MaxVersion { get; private set; }

        public void Read(Stream source)
        {
            this.ApiKey = source.ReadInt16();
            this.MinVersion = source.ReadInt16();
            this.MaxVersion = source.ReadInt16();
        }
    }
}